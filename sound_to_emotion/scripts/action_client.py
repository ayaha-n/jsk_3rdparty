#!/usr/bin/env python3

import rospy
import actionlib
from sound_to_emotion.msg import *

def feedback_cb(feedback):
    rospy.loginfo(f"Feedback: {feedback.status}")

if __name__ == '__main__':
    rospy.init_node('process_audio_client')
    client = actionlib.SimpleActionClient('process_audio', ProcessAudioAction)
    
    rospy.loginfo("Waiting for action server to start...")
    client.wait_for_server()
    rospy.loginfo("Action server started!")

    audio_topic = '/audio/audio'  # ここは送信したいトピックに合わせて調整
    goal = ProcessAudioGoal(audio_topic)
    

    client.send_goal(goal, feedback_cb=feedback_cb)

    client.wait_for_result()
    result = client.get_result()
    rospy.loginfo("Finished")
