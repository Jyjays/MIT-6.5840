def read_data(filename):
    data = {}
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                user = parts[0]
                movies = set(parts[1].split(';'))
                data[user] = movies
                #print(user, movies)
    return data

train_data = read_data('movie_recommend.txt')

def read_test_data(filename):
    data = {}
    testSet_len = 0
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 2:
                user = parts[0]
                movies = set(parts[1:])
            
            # Initialize user if not already in the dictionary
            if user not in data:
                data[user] = {}

            # Add movies and count how many the user has watched
            for movie in movies:
                data[user].setdefault(movie, 0)
                data[user][movie] += 1  # Increase the count for each movie

            testSet_len += len(movies)  # Increment by number of movies the user watched

    return data, testSet_len

test_data, total_movies = read_test_data('testSet.txt')
print(f"Total number of movies in test set: {total_movies}")


# 初始化指标累积值
N = 20
# 准确率和召回率
hit = 0
rec_count = 0
test_count = 0
# 覆盖率
all_rec_movies = set()

for user, movies in train_data.items():
    test_movies = test_data.get(user, {})
    rec_movies = movies
    for movie in rec_movies:
        if movie in test_movies:
            hit += 1
        all_rec_movies.add(movie)
    rec_count += N
    test_count += len(test_movies)
    
precision = hit / (1.0 * rec_count)
recall = hit / (1.0 * test_count)
coverage = len(all_rec_movies) / (1.0 * total_movies)

print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"Coverage: {coverage:.4f}")

    
