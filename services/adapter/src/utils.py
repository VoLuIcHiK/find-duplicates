import datetime
import os

import cv2
import numpy as np
import librosa as lib
from collections import defaultdict


def duplicates(video_scores, audio_scores, video_thresh, audio_thresh):
    is_duplicate, is_hard, duplicate_for = False, False, None
    
    for id_ in list(video_scores.keys()):
        if video_scores[id_] >= video_thresh:
            if audio_scores[id_] >= audio_thresh:
                is_duplicate = True
                duplicate_for = id_
            else:
                is_hard = True
        else:
            if audio_scores[id_] >= audio_thresh:
                is_hard = True
    
    return is_duplicate, is_hard, duplicate_for
                


def filter_by_threshold(data, threshold):
    result = {}
    for n, hits in enumerate(data):
        for hit in hits:
            if hit['distance'] > threshold:
                result[hit['entity']['video_id']] = hit['distance']
    
    return result