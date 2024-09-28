import datetime
import os

import cv2
import numpy as np
import librosa as lib
from collections import defaultdict


def extract_spectrogram(video_path, offset = 0, duration = None):
    n_fft = 1024
    hop_length = 512
    n_mels = 128
    
    audio, sr = lib.core.load(video_path, mono=True, sr=44100, offset=offset, duration=duration)

    audio = lib.resample(audio, orig_sr=sr, target_sr=44100)
    audio = lib.util.normalize(audio)

    spectgram = lib.power_to_db(
        lib.feature.melspectrogram(y=audio, sr=44100, n_fft=n_fft, hop_length=hop_length, n_mels=128)).T
    spectgram = np.concatenate([spectgram, np.zeros((176 - spectgram.shape[0] % 176, 128))])  # zero padding

    # This part is for time step of 125 ms
    '''
    iterations = int(spectgram.shape[0]/11 -0.55)

    spectgram2 = np.zeros((iterations, 1, 176, 128))

    for i in range(iterations):
        spectgram2[i] = spectgram[i*11:i*11+176,:].reshape(1,176,128)
    '''
    
    spectgram = np.concatenate([spectgram, np.zeros((87 - spectgram.shape[0] % 87, 128))])  # zero padding
    spectgram = np.reshape(spectgram, (spectgram.shape[0] // 87, 1, 87, 128))  # shape needed from pytorch
    spectgram = np.concatenate([spectgram[:-1], spectgram[1:]], axis=2)  # 1 sec overlap

    return spectgram.astype(np.float32)


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
        timestamps[i[1]].append(i[-1][0])
    
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



def is_drop_frame(shape, video):
    size = shape[0] * shape[1]
    less = lambda x, y: np.sum(x <= y) / size
    bigger = lambda x, y: np.sum(x >= y) / size
    
    # os.makedirs('dropped_frames', exist_ok=True)

    def is_drop(frame):
        frame = cv2.cvtColor(frame, cv2.COLOR_RGB2HSV)
        h, s, v = frame[:, :, 0], frame[:, :, 1], frame[:, :, 2]
        
        less_h = less(h, 20)
        less_s = less(s, 20)
        
        bigger_v = bigger(v, 200)
        is_white = (less_h > 0.75 and less_s > 0.75 and bigger_v > 0.7) or bigger_v > 0.98 or less_s > 0.98
        if is_white:
            # i = 0
            # while os.path.exists(f'dropped_frames/{video}_{i}.jpg'):
                # i += 1
            # frame = cv2.cvtColor(frame, cv2.COLOR_HSV2RGB)
            # frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
            # cv2.imwrite(f'dropped_frames/{video}_{i}.jpg', frame)
            return True
        
        less_v = less(v, 20)
        is_black = ((less_h + less_s + less_v) / 3) > 0.85 or less_v > 0.98
        if is_black:
            # i = 0
            # while os.path.exists(f'dropped_frames/{video}_{i}.jpg'):
            #     i += 1
            # frame = cv2.cvtColor(frame, cv2.COLOR_HSV2RGB)
            # frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
            # cv2.imwrite(f'dropped_frames/{video}_{i}.jpg', frame)
            return True

        return False

    return is_drop

def business_logic(segments):
    return segments