import json
import numpy as np
from collections import deque
from sklearn.metrics.pairwise import cosine_similarity

# Function to load the papers from the JSON file
def load_papers(file_path):
    try:
        with open(file_path, "r") as file:
            content = file.read()  # Read the entire content
            # Fix the JSON if there are multiple objects, by wrapping the objects in an array
            papers = json.loads('[' + content.replace('}\n{', '},{') + ']')
        return papers
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return []
    except json.JSONDecodeError:
        print("Error: There was an issue decoding the JSON file.")
        return []

# This queue will store the papers the user has liked or disliked (with feedback)
user_queue = deque(maxlen=5)  # Keeps track of the last 5 user interactions

# Function to recommend papers based on what the user has liked/disliked
def recommend_papers(user_queue, papers):
    if len(user_queue) == 0:
        return []
    
    # Calculate the average feature vector of all the papers the user has liked
    avg_features = np.mean([paper['features'] for paper in user_queue], axis=0)

    # Compare the user's average preferences with all the papers to find the most similar ones
    similarities = []
    for paper in papers:
        sim_score = cosine_similarity([avg_features], [paper['features']])[0][0]
        similarities.append((paper['title'], sim_score))
    
    # Sort the papers by similarity score (most similar first)
    similarities.sort(key=lambda x: x[1], reverse=True)
    
    return [paper[0] for paper in similarities[:3]]

# Function to like a paper, updates user queue and returns recommendations
def like_paper(paper_id, papers):
    paper = next((p for p in papers if p["paperId"] == paper_id), None)
    
    if paper is None:
        print(f"Paper with ID {paper_id} not found.")
        return []
    
    # Add the liked paper to the user's queue
    user_queue.append(paper)
    
    # Get recommendations based on the user's current preferences
    recommendations = recommend_papers(user_queue, papers)
    
    return recommendations

# Function to dislike a paper, updates user queue and returns recommendations
def dislike_paper(paper_id, papers):
    global user_queue
    
    # Remove the disliked paper from the user's queue
    user_queue = deque([p for p in user_queue if p["paperId"] != paper_id], maxlen=5)
    
    # Get updated recommendations after the paper was disliked
    recommendations = recommend_papers(user_queue, papers)
    
    return recommendations

# Normalize feature vectors to avoid skewing results due to scale
def normalize_features(papers):
    for paper in papers:
        paper['features'] = paper['features'] / np.linalg.norm(paper['features'])
    return papers

# Load the papers from the JSON file (provide the correct file path here)
file_path = 'FAFWHK-BACK/papers.json2'  # Specify the correct file path to your JSON file
papers = load_papers(file_path)

# Normalize the feature vectors for better similarity computation
papers = normalize_features(papers)

# # Example usage of the recommendation system:
# # Like a paper
# recommendations_after_like = like_paper("000014eb61cb679c8458ba4a0bf7588d85f6e9e2", papers)
# print("Recommendations after liking a paper:", recommendations_after_like)

# # Dislike a paper
# recommendations_after_dislike = dislike_paper("000014eb61cb679c8458ba4a0bf7588d85f6e9e2", papers)
# print("Recommendations after disliking a paper:", recommendations_after_dislike)
