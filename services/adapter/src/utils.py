import datetime
import os

import cv2
import numpy as np
import librosa as lib
from collections import defaultdict


def duplicates(matches):
    """
    Функция для реализации логики проверки на дубликат.

    Args:
        matches (_type_): _description_

    Returns:
        _type_: _description_
    """
    is_duplicate, is_hard, duplicate_for = False, False, None
    if len(matches) == 0:
        return False, False, None
    
    aggregated_matches = {i: np.array(j)[:, 0].mean() for i, j in matches.items()}
    if len(list(filter(lambda x: x > 0.9, aggregated_matches.values()))) > 2:
        return False, True, None
    else:
        best_match = max(aggregated_matches, key=matches.get)
        if aggregated_matches[best_match] > 0.8:
            return True, False, best_match
        else:
            return False, False, None
    
    # aggregated_matches = {i: np.mean(j) for i, j in matches.items()}
    # best_match = max(aggregated_matches, key=matches.get)
    
    # if aggregated_matches[best_match] >= 0.8:
    #     return True, False, best_match
    # elif aggregated_matches[best_match] < 0.8:
    #     if max(matches[best_match]) >= 0.9:
    #         return False, True, None
    # else:
    #     return False, False, None



def intersection(cut1, cut2):
    a1, a2 = cut1
    b1, b2 = cut2
    right_intersec = a1 <= b1 and a2 >= b1
    left_intersec = a2 >= b2 and a1 <= b2
    return right_intersec or left_intersec

def unite(cut1, cut2):
    return min([*cut1, *cut2]), max([*cut1, *cut2])


def filter_by_threshold(data, threshold, seq_length, stride, offsets, frame_numbers):
    offsets = np.array(offsets)
    
    result = []
    for n, hits in enumerate(data):
        for hit in hits:
            if hit['distance'] > threshold:
                video_id, index = '_'.join(hit['entity']['video_id'].split('_')[:-1]), hit['entity']['video_id'].split('_')[-1]
                # q_index = n + int((offsets <= frame_numbers[n * stride]).sum())
                q_index = n
                start_q = int(datetime.timedelta(seconds=q_index * 8).total_seconds())
                stop_q = int(datetime.timedelta(seconds=q_index * 8 + seq_length).total_seconds())
                start_t = int(datetime.timedelta(seconds=int(index) * seq_length).total_seconds())
                stop_t = int(datetime.timedelta(seconds=int(index) * seq_length + seq_length).total_seconds())
        
                result.append([
                    (start_q, stop_q),
                    video_id,
                    (start_t, stop_t),
                    (hit['distance'], 1)
                ])
    return result


def seconds2timestamp(data):
    timestamps = defaultdict(list)
    for i in data:
        timestamps[i[1]].append(i[-1])

    return dict(timestamps)


def result2timestamp(data, seq_len):
    timestamps = []
    for i in data:
        start_q = str(datetime.timedelta(seconds=i[0] * 1))
        stop_q = str(datetime.timedelta(seconds=i[1] * 1 + seq_len))
        start_t = str(datetime.timedelta(seconds=i[3] * seq_len))
        stop_t = str(datetime.timedelta(seconds=i[4] * seq_len + seq_len))
        video_id = f"https://rutube.ru/video/{i[2]}/?t={i[3] * seq_len}&r=plwd"
        
        timestamps.append([
            f"{start_q} - {stop_q}",
            video_id,
            f"{start_t} - {stop_t}",
        ])
    
    return timestamps



def business_logic(segments):
    return segments