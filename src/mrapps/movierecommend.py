import pandas as pd
import numpy as np
import scipy.sparse as sp

# 读取用户相似度数据
def load_similarity_data(file_path):
    # 读取用户相似度数据，只取前3000条
    similarity_df = pd.read_csv(file_path, sep=",", header=None, names=["user1", "user2", "similarity"], nrows=30)
    # 读取用户相似度矩阵，假设文件以空格分隔，格式是: user1, user2, similarity
    # similarity_df = pd.read_csv(file_path, sep=",", header=None, names=["user1", "user2", "similarity"])
    return similarity_df

# 根据用户相似度构建稀疏相似度矩阵
def build_similarity_matrix(similarity_df, num_users):
    # 创建一个稀疏矩阵来存储相似度
    row = []
    col = []
    data = []

    for _, row_data in similarity_df.iterrows():
        user1 = int(row_data['user1']) - 1
        user2 = int(row_data['user2']) - 1
        similarity = row_data['similarity']
        
        # 将数据添加到稀疏矩阵的行、列、数据列表中
        row.append(user1)
        col.append(user2)
        data.append(similarity)

    # 创建稀疏矩阵
    similarity_matrix = sp.csr_matrix((data, (row, col)), shape=(num_users, num_users))

    return similarity_matrix

# 获取与目标用户最相似的K个用户
def get_top_k_similar_users(similarity_matrix, user_id, K):
    # 获取当前用户与其他所有用户的相似度
    user_similarity = similarity_matrix[user_id]
    
    # 获取与目标用户最相似的K个用户的索引
    similar_users = np.argsort(user_similarity)[::-1][:K]
    
    return similar_users

# 根据相似用户的评分计算目标用户的预测评分
def predict_score(user_id, movie_id, ratings, similarity_matrix, K):
    similar_users = get_top_k_similar_users(similarity_matrix, user_id, K)
    weighted_sum = 0.0
    similarity_sum = 0.0
    
    # 查找与相似用户评分该电影的记录，并计算加权评分
    for similar_user in similar_users:
        similar_user_ratings = ratings[ratings['UserID'] == similar_user + 1]  # 获取相似用户的评分
        movie_rating = similar_user_ratings[similar_user_ratings['MovieID'] == movie_id]
        
        if not movie_rating.empty:
            # 获取相似度，并计算加权评分
            rating = movie_rating['Rating'].values[0]
            similarity = similarity_matrix[user_id, similar_user]
            weighted_sum += similarity * rating
            similarity_sum += abs(similarity)
    
    # 如果相似度和为0，表示没有有效的相似用户评分该电影，返回0
    if similarity_sum == 0:
        return 0
    else:
        return weighted_sum / similarity_sum

# 生成推荐列表
def recommend_movies(ratings, similarity_matrix, K, num_users, num_movies):
    recommendations = []
    
    for user_id in range(num_users):
        rated_movies = ratings[ratings['UserID'] == user_id + 1]['MovieID'].values
        for movie_id in range(1, num_movies + 1):
            # 如果该用户已经评分过该电影，跳过
            if movie_id in rated_movies:
                continue
            predicted_score = predict_score(user_id, movie_id, ratings, similarity_matrix, K)
            recommendations.append((user_id + 1, movie_id, predicted_score))
    
    # 按照预测评分降序排序
    recommendations = sorted(recommendations, key=lambda x: x[2], reverse=True)
    
    return recommendations

# 读取数据
def readdata(ratings_file):
    ratings = pd.read_table(ratings_file, sep='::', names=['UserID', 'MovieID', 'Rating', 'Timestamp'], engine='python', encoding='ISO-8859-1')
    return ratings

def main():
    similarity_df = load_similarity_data("user_matrix.csv")
    ratings = readdata("ml-latest-small/ratings.csv")

    num_users = ratings['UserID'].nunique()
    num_movies = ratings['MovieID'].nunique()
    
    # 构建相似度矩阵
    similarity_matrix = build_similarity_matrix(similarity_df, num_users)
    
    # 生成推荐列表
    K = 3  # 选择最相似的3个用户
    recommendations = recommend_movies(ratings, similarity_matrix, K, num_users, num_movies)
    
    # 打印推荐结果
    print("UserID, MovieID, Predicted Score")
    for user_id, movie_id, predicted_score in recommendations[:10]:  # 显示前10个推荐
        print(f"{user_id}, {movie_id}, {predicted_score:.4f}")

if __name__ == '__main__':
    main()
