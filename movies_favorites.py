import os
import sys
import csv

from threading import Thread
from queue import Queue
from tqdm import tqdm
from imdb import IMDb

class IMDbThreadWorker(Thread):
    """
    IMDbThreadWorker class
    Main class for IMDb(Internet Movie Database) API requests using threads
    __init__:
    @param queue_work: Queue with information from the given list.
    @param queue_data: Queue with movie data obtained from the imdb api.
    @return: IMDbThreadWorker object
    """
    def __init__(self, queue_work, queue_data):
        Thread.__init__(self)
        self.queue_work = queue_work
        self.queue_data = queue_data
        self.movies = []

    def get_movie(self, name):
        """
        Get information of each movie using the imdb api.
        @param name: movie name.
        @return: movie data information
        """
        imdb_api = IMDb()
        search_movie = imdb_api.search_movie(name)
        for number, search in enumerate(search_movie):
            if search.data["kind"] == "movie":
                movie_id = search_movie[number].movieID
                movie = imdb_api.get_movie(movie_id)
                break
        return movie

    def run(self):
        """
        Thread queue work.
        @return: Completed work queue
        """
        while True:
            number, name = self.queue_work.get()
            try:
                self.queue_data.put((number, self.get_movie(name)))
            finally:
                self.queue_work.task_done()

class MoviesFavorites(object):
    """
    MoviesFavorites class
    Main class MoviesFavorites to initialize
    __init__:
    @param filename: list name in text file.
    @return: MoviesFavorites object
    """
    def __init__(self, filename=""):
        self.filename = filename
        self.threads = 16
        self.fieldnames = [
            "original title", "localized title(Brasil)", "year",
            "rating", "votes", "top 250 rank", "run time", 
            "genres", "kind", "languages"
        ]

    def print_help(self):
        """
        Print usage.
        @return: code 0
        """
        print("usage: python movies_favorites.py [list.txt]")
        return 0

    def get_readlines(self):
        """
        Open text file and get the lines.
        @return: file lines
        """
        with open(os.path.join(self.filename), "r", encoding="utf-8") as file:
            file_readlines = file.readlines()
            file.close()
        return file_readlines

    def get_runtime_formatted(self, runtime):
        """
        Simple formatted runtime
        @param runtime: movie runtime
        @return: runtime formatted
        """
        return "{}h {}m".format(int(runtime / 60), int(runtime % 60))

    def generate_queue_and_movies(self):
        """
        Create a queue using class IMDbThreadWorker and get the movies data.
        @return: movies data information
        """
        queue_work = Queue()
        queue_data = Queue()
        lines = self.get_readlines()
        for number, line in enumerate(lines):
            movie_name = line.replace("\n", "")
            queue_work.put((number, movie_name))
        for i in range(self.threads):
            worker = IMDbThreadWorker(queue_work, queue_data)
            worker.daemon = True
            worker.start()
        movies = []
        for i in tqdm(range(len(lines)), desc="movies"):
            movies.append(queue_data.get())
        return movies

    def generate_movies_data(self, movies_queue):
        """
        Create array with movie information.
        @param movies_queue: Queue with all movies.
        @return: movies information
        """
        data = []
        for movie in sorted(movies_queue, key=lambda m: m[0]):
            movie = movie[1]
            data.append([{
                "original title": movie.data["original title"] if movie.data.get("original title") else movie.data["title"],
                "localized title(Brasil)": movie.data["localized title"] if movie.data.get("localized title") else original_title,
                "year": movie.data["year"] if movie.data.get("year") else "None",
                "rating": movie.data["rating"] if movie.data.get("rating") else "None",
                "votes": movie.data["votes"] if movie.data.get("votes") else "None",
                "top 250 rank": movie.data["top 250 rank"] if movie.data.get("top 250 rank") else "None",
                "run time": self.get_runtime_formatted(int(movie.data["runtimes"][0])) if movie.data.get("runtimes") else "None",
                "genres": ", ".join(movie.data["genres"]) if movie.data.get("genres") else "None",
                "kind": movie.data["kind"] if movie.data.get("kind") else "None",
                "languages": movie.data["languages"][0] if movie.data.get("languages") else "None"
            }])
        return data

    def generate_csv(self, rows):
        """
        Create cvs file.
        @param rows: Array with all movies data information.
        @return: True
        """
        with open(os.path.join(self.filename.replace(".txt", ".csv")), "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerows(row)
            file.close()
        return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(MoviesFavorites().print_help())
    movies_favorites = MoviesFavorites(sys.argv[1])
    movies_queue = movies_favorites.generate_queue_and_movies()
    movies_data = movies_favorites.generate_movies_data(movies_queue)
    movies_csv = movies_favorites.generate_csv(movies_data)
    sys.exit(0)
