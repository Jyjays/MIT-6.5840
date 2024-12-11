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
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                user = parts[0]
                movies = set(parts[1].split(','))
                data[user] = movies
    return data

test_data = read_test_data('eval_result.txt')

# 初始化指标累积值
N = 10
# 准确率和召回率
hit = 0
rec_count = 0
test_count = 0
# 覆盖率
all_rec_movies = set()

for user, movies in train_data.items():
    test_movies = test_data.get(user, {})
    # 获取推荐结果
    rec_movies = movies
    for movie in rec_movies:
        if movie in test_movies:
            hit += 1
        all_rec_movies.add(movie)
    rec_count += N
    test_count += len(test_movies)

precision = hit / (1.0 * rec_count)
recall = hit / (1.0 * test_count)
coverage = len(all_rec_movies) / len(train_data)
print('precision=%.4f\trecall=%.4f\tcoverage=%.4f' % (precision, recall, coverage))
