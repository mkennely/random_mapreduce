from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdwon(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_num_ratings,
                reducer=self.reducer_count_ratings
            ),
            MRStep(
                reducer=self.reducer_sorted_output
            )
        ]
    
    # returns a movie ID and number 1 per each record which will later be combined and summed to get how many ratings per movie
    # this is done on big data so that we can be more efficient by only selecting/using data that we need 
    # input could look like `333,555,4.5,20190601`  & `222, 9, 3.7, 20190522` & `123, 9, 0, 20190302`
    # output would be condensed to {333: 1, 9:1, 9:1}
    def mapper_get_num_ratings(self, _, line):
        user_id, movie_id, rating, time_stamp = line.split('\t')
        yield movie_id, 1

    # input could look like {333:1,
    #                          9: 1, 1}
    # sums the number of ratings per movie
    # returns number of ratings as key and movie as values so that it'll automatically sort the movies based of off the number of ratings
    # output can look like { 2: 9
    #                        1: 333} 
    # where the key is the number of ratings and value is the movies with that number of ratings
    def reducer_count_ratings(self, movie, values):
        yield str(sum(values)).zfill(5), movie  # Left pad with 0s bc these are treated as strings and this forces them to be sorted correctly

    # we do this to get the list of movies ordered by the number of ratings
    # MapReduce automatically does a sort/merge for us on the key-value pairs when passing that info to a reducer
    # Since multiple movies could have the same number of ratings we do a for loop to iterate over them
    def reducer_sorted_output(self, count, movies)
        for movie in movies:
            yield movie, count


if __name__ == '__main__':
    RatingsBreakdwon.run()