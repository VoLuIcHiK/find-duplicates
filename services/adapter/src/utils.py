import datetime
import os

import cv2
import numpy as np
import librosa as lib
from collections import defaultdict


def duplicates(video_scores, audio_scores, video_thresh, audio_thresh):
    is_duplicate, is_hard, duplicate_for = False, False, None
    
    for id_, val in dict(sorted(video_scores.items(), key = lambda x: x[1])).items():
        if video_scores[id_] >= video_thresh:
            if audio_scores[id_] >= audio_thresh and audio_scores[id_] <= 1.0:
                is_duplicate = True
                duplicate_for = id_
            else:
                if audio_scores[id_] > 1.0:
                    if video_scores[id_] >= 0.8:
                        is_duplicate = True
                        duplicate_for = id_
                else:
                    is_hard = True
            break
        else:
            if audio_scores[id_] >= audio_thresh and audio_scores[id_] <= 1.0:
                is_hard = True
            break
    
    return is_duplicate, is_hard, duplicate_for
                


def filter_by_threshold(data, threshold):
    result = {}
    for n, hits in enumerate(data):
        for hit in hits:
            if hit['distance'] > threshold:
                result[hit['entity']['video_id'].split('/')[-1].split('.')[0]] = hit['distance']
    
    return result