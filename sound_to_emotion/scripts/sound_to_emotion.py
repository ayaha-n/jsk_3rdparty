#!/usr/bin/env python3

import rospy
import actionlib
from sound_to_emotion.msg import *
from hume import HumeStreamClient
from hume.models.config import BurstConfig, ProsodyConfig
from hume import StreamSocket
import asyncio
import traceback
import time
from base64 import b64encode
from io import BytesIO
import soundfile as sf
import numpy as np
from pydub import AudioSegment
import array
from threading import Lock
from audio_common_msgs.msg import AudioData

class AudioBuffer(object):

    def __init__(self, topic_name='~audio',
                 input_sample_rate=16000,
                 window_size=10.0,
                 bitdepth=16,
                 n_channel=1, target_channel=0,
                 get_latest_data=False,
                 discard_data=False,
                 auto_start=False):
        self.is_subscribing = True
        self.get_latest_data = get_latest_data
        self.discard_data = discard_data
        self._window_size = window_size
        self.audio_buffer_len = int(self._window_size * input_sample_rate)
        self.lock = Lock()
        self.bitdepth = bitdepth
        self.n_channel = n_channel
        self.target_channel = min(self.n_channel - 1, max(0, target_channel))
        self.input_sample_rate = input_sample_rate
        self.type_code = {}
        for code in ['b', 'h', 'i', 'l']:
            self.type_code[array.array(code).itemsize] = code

        self.dtype = self.type_code[self.bitdepth / 8]
        self.audio_buffer = np.array([], dtype=self.dtype)

        self.max_value = 2 ** (self.bitdepth - 1) - 1

        self.topic_name = topic_name

        if auto_start:
            self.subscribe()

    def __len__(self):
        return len(self.audio_buffer)

    @property
    def window_size(self):
        return self._window_size

    @window_size.setter
    def window_size(self, size):
        with self.lock:
            self._window_size = size
            self.audio_buffer_len = int(self._window_size
                                        * self.input_sample_rate)
            self.audio_buffer = np.array([], dtype=self.dtype)

    @staticmethod
    def from_rosparam(**kwargs):
        n_channel = rospy.get_param('~n_channel', 1)
        target_channel = rospy.get_param('~target_channel', 0)
        mic_sampling_rate = rospy.get_param('~mic_sampling_rate', 16000)
        bitdepth = rospy.get_param('~bitdepth', 16)
        return AudioBuffer(input_sample_rate=mic_sampling_rate,
                           bitdepth=bitdepth,
                           n_channel=n_channel,
                           target_channel=target_channel,
                           **kwargs)

    def subscribe(self):
        self.audio_buffer = np.array([], dtype=self.dtype)
        self.sub_audio = rospy.Subscriber(
            self.topic_name, AudioData, self.audio_cb)

    def unsubscribe(self):
        self.sub_audio.unregister()

    def _read(self, size, normalize=False):
        with self.lock:
            if self.get_latest_data:
                audio_buffer = self.audio_buffer[-size:]
            else:
                audio_buffer = self.audio_buffer[:size]
                if self.discard_data:
                    self.audio_buffer = self.audio_buffer[size:]
        if normalize is True:
            audio_buffer = audio_buffer / self.max_value
        return audio_buffer

    def sufficient_data(self, size):
        return len(self.audio_buffer) < size

    def read(self, size=None, wait=False, normalize=False):
        if size is None:
            size = self.audio_buffer_len
        size = int(size * self.input_sample_rate)
        while wait is True \
                and not rospy.is_shutdown() and len(self.audio_buffer) < size:
            rospy.sleep(0.001)
        return self._read(size, normalize=normalize)

    def close(self):
        try:
            self.sub_audio.unregister()
        except Exception:
            pass
        self.audio_buffer = np.array([], dtype=self.dtype)

    def audio_cb(self, msg):
        audio_buffer = np.frombuffer(msg.data, dtype=self.dtype)
        audio_buffer = audio_buffer[self.target_channel::self.n_channel]
        with self.lock:
            self.audio_buffer = np.append(
                self.audio_buffer, audio_buffer)
            self.audio_buffer = self.audio_buffer[
                -self.audio_buffer_len:]
            
class ProcessAudioServer:
    def __init__(self):
        # Actionサーバーの初期化
        self.server = actionlib.SimpleActionServer(
            'process_audio', ProcessAudioAction, execute_cb=self.execute_callback, auto_start=False)
        self.server.start()
        # シャットダウン処理を登録
        rospy.on_shutdown(self.on_shutdown)

    def on_shutdown(self):
        rospy.loginfo("Shutting down ProcessAudioServer...")
        if self.audio_buffer is not None:
            self.audio_buffer.unsubscribe()
            self.audio_buffer.audio_buffer = np.array([], dtype=self.audio_buffer.dtype)
            rospy.loginfo("Audio buffer unsubscribed and cleared.")
        
    async def encode_audio(self, audio_buffer):
        """音声データをエンコードして送信バイト列を生成"""
        wav_outpath = '/tmp/hoge.wav'
        bytes_io = BytesIO()

        # バッファから音声データを読み込んでWAVとして保存
        with sf.SoundFile(wav_outpath, mode='w',
                          samplerate=audio_buffer.input_sample_rate,
                          channels=audio_buffer.n_channel,
                          format='wav') as f:
            tmp = audio_buffer.read()
            f.write(tmp)

        # 音声ファイルをバイト列に変換
        segment = AudioSegment.from_file(file=wav_outpath, format="wav")
        segment.export(bytes_io, format="wav")
        return b64encode(bytes_io.read())

    async def send_audio(self, audio_buffer, client, configs):
        """音声データを送信する非同期タスク"""
        try:
            async with client.connect(configs) as socket:
                socket: StreamSocket
                while True:
                    result = None
                    await socket.reset_stream()
                    send_bytes = await self.encode_audio(audio_buffer)
                    rospy.loginfo(f"Sending audio of length: {len(send_bytes)}")

                    if len(send_bytes) <= 100:
                        break

                    result = await socket.send_bytes(send_bytes)
                    rospy.loginfo(f"Result: {result}")
                    await asyncio.sleep(1.0)  # 1秒おきに処理
        except Exception as e:
            rospy.logerr(f"Error during audio processing: {str(e)}")
            return None

    def execute_callback(self, goal):
        """Actionサーバーのコールバックメソッド"""
        feedback = ProcessAudioFeedback()
        result = ProcessAudioResult()

        # APIキーの設定をチェック
        api_key = rospy.get_param('~api_key', '')
        if not api_key:
            rospy.logerr("API key has not been set")
            self.server.set_aborted()
            return

        # AudioBufferの設定
        audio_buffer = AudioBuffer(topic_name=goal.audio_topic, window_size=2, auto_start=True)

        # HumeClientと設定の準備
        client = HumeStreamClient(api_key)
        configs = [BurstConfig(), ProsodyConfig()]

        # 非同期タスクの開始
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        task = loop.create_task(self.send_audio(audio_buffer, client, configs))

        # 非同期タスクが完了するまで待機
        loop.run_until_complete(task)

        # 結果を返す
        if task.result() is not None:
            result.result = task.result()  # 結果として送信したバイト列や処理結果を設定
            self.server.set_succeeded(result)
        else:
            self.server.set_aborted()

if __name__ == '__main__':
    rospy.init_node('process_audio_server')
    server = ProcessAudioServer()
    rospy.spin()
