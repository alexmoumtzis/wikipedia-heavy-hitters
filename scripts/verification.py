import bz2

path = r"C:\Users\alexm\wiki-heavy-hitters\data\pageviews-20250101-user.bz2"

with bz2.open(path, "rt", encoding="utf-8") as f:
    for _ in range(10):
        print(f.readline())