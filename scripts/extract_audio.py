import subprocess
import os
from loguru import logger
from pathlib import Path
import argparse

log_level = "DEBUG"
log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
logger.add("file_audio.log", level=log_level, format=log_format, colorize=False, backtrace=True, diagnose=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--video_folder', type=str, help='Video dataset folder')
    parser.add_argument('--audio_folder', type=str, help='Audio dataset')
    args = parser.parse_args()
    videos = list(Path(args.video_folder).rglob('*.mp4'))
    cmd = "ffmpeg -i {} -vn -acodec pcm_s16le -ar 16000 -ac 1 {}"
    os.makedirs(args.audio_folder, exist_ok=True)
    for v in videos[:1]:
        try:
            audio_name, _ = os.path.splitext(v.name)
            subprocess.call(cmd.format(str(v), str(Path(args.audio_folder) / f'{audio_name}.wav')), shell=True)
        except Exception as e:
            logger.error(f'Unable to extract audio from file {v}. Error:{e}')
