import sys
import csv
import time
from threading import Thread
from queue import Queue
from tqdm import tqdm
from imdb import IMDb

class IMDbWorker(Thread):
    def __init__(self, qs, qw):
        Thread.__init__(self)
        self.qs = qs
        self.qw = qw
        self.movies = []

    def get_movie(self, name):
        ia = IMDb()
        search = ia.search_movie(name)
        for enum, s in enumerate(search):
            if s.data["kind"] == "movie":
                movie_id = search[enum].movieID
                movie = ia.get_movie(movie_id)
                break
        return movie

    def run(self):
        while True:
            nb, name = self.qs.get()
            try:
                self.qw.put((nb, self.get_movie(name)))
            finally:
                self.qs.task_done()

def print_help():
    print("usage: python movies_favorites.py [list.txt]")
    return 1

def get_readlines(file):
    lines = open(file, "r", encoding="utf-8").readlines()
    return lines

def get_runtime(runtime):
    hours = int(runtime / 60)
    minutes = int(runtime % 60)
    return "{}h {}m".format(hours, minutes)

def generate_queue_and_movies(file_list):
    queue_w = Queue()
    queue_s = Queue()
    lines = get_readlines(file_list)
    for enum, line in enumerate(lines):
        movie_name = line.replace("\n", "")
        queue_w.put((enum, movie_name))
    for w in range(32):
        worker = IMDbWorker(queue_w, queue_s)
        worker.daemon = True
        worker.start()
    movies = []
    for i in tqdm(range(len(lines)), desc='movies'):
        movies.append(queue_s.get())
    return movies

def generate_items(movies):
    items = []
    for movie in sorted(movies, key=lambda l: l[0]):
        movie = movie[1]
        original_title = movie.data["original title"] if movie.data.get("original title") else movie.data["title"]
        localized_title = movie.data["localized title"] if movie.data.get("localized title") else original_title
        year = movie.data["year"] if movie.data.get("year") else "None"
        rating = movie.data["rating"] if movie.data.get("rating") else "None"
        votes = movie.data["votes"] if movie.data.get("votes") else "None"
        top_250_rank = movie.data["top 250 rank"] if movie.data.get("top 250 rank") else "None"
        run_time = get_runtime(int(movie.data["runtimes"][0])) if movie.data.get("runtimes") else "None"
        genres = ", ".join(movie.data["genres"]) if movie.data.get("genres") else "None"
        kind = movie.data["kind"] if movie.data.get("kind") else "None"
        languages = movie.data["languages"][0] if movie.data.get("languages") else "None"
        items.append([
            {
                "original title": original_title,
                "localized title(Brasil)": localized_title,
                "year": year,
                "rating": rating,
                "votes": votes,
                "top 250 rank": top_250_rank,
                "run time": run_time,
                "genres": genres,
                "kind": kind,
                "languages": languages
            }
        ])
    return items

def generate_csv(rows):
    field_names = [
        "original title", "localized title(Brasil)", "year",
        "rating", "votes", "top 250 rank", "run time", 
        "genres", "kind", "languages"
    ]
    with open("list_{time}.csv".format(time=int(time.time())), "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerows(row)
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(print_help())
    movies = generate_queue_and_movies(sys.argv[1])
    items = generate_items(movies)
    sys.exit(generate_csv(items))