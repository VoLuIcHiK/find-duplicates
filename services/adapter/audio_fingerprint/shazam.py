import uuid
import numpy as np
import os, pickle
from pydub import AudioSegment
from pathlib import Path
from moviepy.editor import VideoFileClip
from scipy.signal import spectrogram
from scipy.ndimage import maximum_filter
from tqdm import tqdm
from . import sh_opt


def mp4_to_wav(mp4_file, wav_file):
    """
    Converts a video file (mp4) to a wav audio file.

    :param mp4_file: Path to the video file
    :param wav_file: Path where to save the audio
    """
    assert mp4_file[-3:] == 'mp4'

    video = VideoFileClip(mp4_file, audio_fps=44100 // 4)
    audio = video.audio

    audio.write_audiofile(wav_file)


def file_to_spectrogram(filename):
    """
    Generates a spectrogram with the specified SAMPLE_RATE and FFT_WINDOW_SIZE.

    :param filename: Path to the audio file
    :returns:   f - np.array of frequencies
                t - np.array of time segments
                Sxx - np.array of power (magnitude) for each time/frequency pair
    """
    a = AudioSegment.from_file(filename).set_channels(1).set_frame_rate(sh_opt.SAMPLE_RATE)
    audio = np.frombuffer(a.raw_data, np.int16)
    nperseg = int(sh_opt.SAMPLE_RATE * sh_opt.FFT_WINDOW_SIZE)
    return spectrogram(audio, sh_opt.SAMPLE_RATE, nperseg=nperseg)


def find_peaks(Sxx):
    """
    Finds peaks in the spectrogram.

    :param Sxx: Spectrogram
    :returns: List of tuples (peaks in the form of (frequency, time))
    """
    data_max = maximum_filter(Sxx, size=sh_opt.PEAK_BOX_SIZE, mode='constant', cval=0.0)
    peak_goodmask = (Sxx == data_max)  # пики помечаем значениями True
    y_peaks, x_peaks = peak_goodmask.nonzero()
    peak_values = Sxx[y_peaks, x_peaks]
    i = peak_values.argsort()[::-1]
    j = [(y_peaks[idx], x_peaks[idx]) for idx in i]
    total = Sxx.shape[0] * Sxx.shape[1]

    peak_target = int((total / (sh_opt.PEAK_BOX_SIZE ** 2)) * sh_opt.POINT_EFFICIENCY)  # Сколько пиков оставим
    return j[:peak_target]


def idxs_to_tf_pairs(idxs, t, f):
    """
    Converts time/frequency indexes to actual time/frequency pairs.

    :param idxs: Indexes of peaks
    :param t: Time array
    :param f: Frequency array
    :returns: Array of time/frequency pairs
    """
    return np.array([(f[i[0]], t[i[1]]) for i in idxs])


def hash_point_pair(p1, p2):
    """
    Converts a pair of points (time, frequency) into a hash.

    :param p1: First point (frequency, time)
    :param p2: Second point (frequency, time)
    :returns: Hashed value of the pair
    """
    return hash((p1[0], p2[0], p2[1] - p2[1]))


def target_zone(anchor, points, width, height, t):
    """
    Generates the target zone to form pairs of peaks.

    :param anchor: The anchor point for creating pairs
    :param points: List of possible points
    :param width: Width of the target zone
    :param height: Height of the target zone
    :param time_offset: Seconds between the start of the target zone and the anchor point
    :returns: Generator for all valid point pairs
    """
    x_min = anchor[1] + t
    x_max = x_min + width
    y_min = anchor[0] - (height * 0.5)
    y_max = y_min + height
    for point in points:
        if point[0] < y_min or point[0] > y_max:
            continue
        if point[1] < x_min or point[1] > x_max:
            continue
        yield point


def hash_points(points):
    """
    Creates hashes for peaks (forms pairs of peaks from the target zone for each peak).

    :param points: List of peaks
    :returns: List of tuples in the form (hash of the peak pair, time difference, video_id)
    """
    hashes = []
    for anchor in points:
        for target in target_zone(
                anchor, points, sh_opt.TARGET_T, sh_opt.TARGET_F, sh_opt.TARGET_START
        ):
            hashes.append((
                hash_point_pair(anchor, target),
                anchor[1],
                target[1],
            ))
    return hashes


def fingerprint_file(filename):
    """
    Generates the fingerprint (hash) from the audio file.

    :returns: Output of hash_points function
    """
    f, t, Sxx = file_to_spectrogram(filename)
    peaks = find_peaks(Sxx)
    peaks = idxs_to_tf_pairs(peaks, t, f)
    return hash_points(peaks)


def compare_fingerprints(file1, file2):
    """
    Compares the fingerprints of two files.

    :param file1: Path to the first audio file
    :param file2: Path to the second audio file
    :returns: A similarity score between the two files
    """
    # Generate fingerprints for both files
    fingerprints1 = fingerprint_file(file1)
    fingerprints2 = fingerprint_file(file2)
    
    # Extract only the hashes from the fingerprints
    hashes1 = {fp[0] for fp in fingerprints1}
    hashes2 = {fp[0] for fp in fingerprints2}

    # Calculate the number of common hashes
    common_hashes = hashes1.intersection(hashes2)
    
    # Calculate similarity score
    total_hashes = len(hashes1.union(hashes2))
    similarity = len(common_hashes) / total_hashes if total_hashes > 0 else 0

    print(f"Number of common hashes: {len(common_hashes)}")
    print(f"Total unique hashes: {total_hashes}")
    print(f"Similarity score: {similarity * 100:.2f}%")
    
    return similarity

if __name__ == '__main__':
    # Example usage:
    file1 = "2cf8f595-acd3-489f-b4ca-86d02c4e1eb2.wav"
    file2 = "390f73b5-6e10-4db6-978d-03cf06d2cc39.wav"
    # similarity = compare_fingerprints(file1, file2)
    t = fingerprint_file('390f73b5-6e10-4db6-978d-03cf06d2cc39.wav')
