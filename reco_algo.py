from datetime import datetime
import numpy as np
from collections import defaultdict

class UserPaperInteractions:
    def __init__(self):
        """Initialize the user interaction tracking system"""
        # Store user interactions with papers and authors
        self.user_interactions = defaultdict(lambda: {
            'view_count': defaultdict(int),      # How many times user viewed each paper
            'reading_time': defaultdict(float),  # Total time spent reading each paper (seconds)
            'last_viewed': defaultdict(datetime),# Last time user viewed each paper
            'bookmarks': set(),                 # Papers user has bookmarked
            'downloads': set(),                 # Papers user has downloaded
            'show_less': set(),                 # Papers marked as "show less"
            'show_more': set(),                 # Papers marked as "show more"
            'followed_authors': set(),          # Authors the user follows
            'blocked_authors': set(),           # Authors the user has blocked
            'paper_ratings': defaultdict(int)   # Implicit rating based on show more/less (-1, 0, 1)
        })
        
        # Store current reading sessions
        self.active_sessions = defaultdict(dict)
        
        # Cache for author papers
        self.author_papers = defaultdict(set)
    
    def start_paper_view(self, user_id, paper_id, timestamp=None):
        """Record when a user starts viewing a paper"""
        if timestamp is None:
            timestamp = datetime.now()
            
        self.active_sessions[user_id][paper_id] = timestamp
        self.user_interactions[user_id]['view_count'][paper_id] += 1
        self.user_interactions[user_id]['last_viewed'][paper_id] = timestamp
    
    def end_paper_view(self, user_id, paper_id, timestamp=None):
        """Record when a user stops viewing a paper"""
        if timestamp is None:
            timestamp = datetime.now()
            
        if user_id in self.active_sessions and paper_id in self.active_sessions[user_id]:
            start_time = self.active_sessions[user_id][paper_id]
            duration = (timestamp - start_time).total_seconds()
            
            # Only count if duration is reasonable (e.g., between 10 seconds and 2 hours)
            if 10 <= duration <= 7200:
                self.user_interactions[user_id]['reading_time'][paper_id] += duration
                
            del self.active_sessions[user_id][paper_id]
    
    def add_bookmark(self, user_id, paper_id):
        """Record when a user bookmarks a paper"""
        self.user_interactions[user_id]['bookmarks'].add(paper_id)
    
    def remove_bookmark(self, user_id, paper_id):
        """Record when a user removes a bookmark"""
        self.user_interactions[user_id]['bookmarks'].discard(paper_id)
    
    def add_download(self, user_id, paper_id):
        """Record when a user downloads a paper"""
        self.user_interactions[user_id]['downloads'].add(paper_id)
    
    def show_more(self, user_id, paper_id, author_id):
        """Record when a user wants to see more similar papers"""
        self.user_interactions[user_id]['show_more'].add(paper_id)
        self.user_interactions[user_id]['paper_ratings'][paper_id] = 1
        # Update author papers cache
        self.author_papers[author_id].add(paper_id)
    
    def show_less(self, user_id, paper_id, author_id):
        """Record when a user wants to see fewer similar papers"""
        self.user_interactions[user_id]['show_less'].add(paper_id)
        self.user_interactions[user_id]['paper_ratings'][paper_id] = -1
        # Update author papers cache
        self.author_papers[author_id].add(paper_id)
    
    def follow_author(self, user_id, author_id):
        """Record when a user follows an author"""
        self.user_interactions[user_id]['followed_authors'].add(author_id)
        # Remove from blocked if present
        self.user_interactions[user_id]['blocked_authors'].discard(author_id)
    
    def unfollow_author(self, user_id, author_id):
        """Record when a user unfollows an author"""
        self.user_interactions[user_id]['followed_authors'].discard(author_id)
    
    def block_author(self, user_id, author_id):
        """Record when a user blocks an author"""
        self.user_interactions[user_id]['blocked_authors'].add(author_id)
        # Remove from followed if present
        self.user_interactions[user_id]['followed_authors'].discard(author_id)
    
    def unblock_author(self, user_id, author_id):
        """Record when a user unblocks an author"""
        self.user_interactions[user_id]['blocked_authors'].discard(author_id)
    
    def get_user_paper_score(self, user_id, paper_id, author_id):
        """Calculate an engagement score for a specific paper"""
        interactions = self.user_interactions[user_id]
        
        # Return very low score for papers by blocked authors
        if author_id in interactions['blocked_authors']:
            return -1000
        
        # Weight factors for different interactions
        weights = {
            'view_count': 0.15,
            'reading_time': 0.25,
            'bookmark': 0.15,
            'download': 0.15,
            'show_more': 0.15,
            'show_less': -0.15,
            'followed_author': 0.15
        }
        
        score = 0
        
        # Normalize view count (cap at 5 views)
        view_score = min(interactions['view_count'][paper_id] / 5, 1.0)
        score += view_score * weights['view_count']
        
        # Normalize reading time (cap at 30 minutes)
        reading_time = interactions['reading_time'][paper_id]
        time_score = min(reading_time / 1800, 1.0)
        score += time_score * weights['reading_time']
        
        # Binary scores
        if paper_id in interactions['bookmarks']:
            score += weights['bookmark']
        if paper_id in interactions['downloads']:
            score += weights['download']
        if paper_id in interactions['show_more']:
            score += weights['show_more']
        if paper_id in interactions['show_less']:
            score += weights['show_less']
        if author_id in interactions['followed_authors']:
            score += weights['followed_author']
            
        return score
    
    def get_recommended_papers(self, user_id, all_papers, n_recommendations=10):
        """Get paper recommendations based on user interaction patterns"""
        if user_id not in self.user_interactions:
            return sorted(
                all_papers,
                key=lambda x: x['publicationDate'],
                reverse=True
            )[:n_recommendations]
        
        interactions = self.user_interactions[user_id]
        
        # Filter out papers by blocked authors
        filtered_papers = [
            paper for paper in all_papers
            if paper.get('authorId') not in interactions['blocked_authors']
        ]
        
        # Prioritize papers from followed authors
        followed_author_papers = [
            paper for paper in filtered_papers
            if paper.get('authorId') in interactions['followed_authors']
        ]
        
        # Calculate scores for remaining papers
        paper_scores = []
        for paper in filtered_papers:
            paper_id = paper['paperId']
            author_id = paper.get('authorId')
            
            # Skip papers the user has explicitly marked as "show less"
            if paper_id in interactions['show_less']:
                continue
                
            score = self.get_user_paper_score(user_id, paper_id, author_id)
            paper_scores.append((paper, score))
        
        # Sort by score
        sorted_papers = sorted(paper_scores, key=lambda x: x[1], reverse=True)
        
        # Combine recommendations: 
        # 1. Papers from followed authors
        # 2. Highest scoring papers
        recommendations = []
        
        # Add papers from followed authors first (up to half of recommendations)
        followed_limit = n_recommendations // 2
        recommendations.extend(followed_author_papers[:followed_limit])
        
        # Fill remaining slots with highest scoring papers
        remaining_slots = n_recommendations - len(recommendations)
        for paper, score in sorted_papers:
            if paper not in recommendations:
                recommendations.append(paper)
                if len(recommendations) >= n_recommendations:
                    break
        
        return recommendations
    
    def get_user_stats(self, user_id):
        """Get statistics about a user's reading behavior"""
        if user_id not in self.user_interactions:
            return None
            
        interactions = self.user_interactions[user_id]
        
        return {
            'total_papers_viewed': len(interactions['view_count']),
            'total_reading_time': sum(interactions['reading_time'].values()),
            'average_reading_time': (
                sum(interactions['reading_time'].values()) / 
                len(interactions['reading_time']) if interactions['reading_time'] else 0
            ),
            'bookmarked_papers': len(interactions['bookmarks']),
            'downloaded_papers': len(interactions['downloads']),
            'followed_authors': len(interactions['followed_authors']),
            'blocked_authors': len(interactions['blocked_authors']),
            'show_more_count': len(interactions['show_more']),
            'show_less_count': len(interactions['show_less']),
            'most_viewed_papers': sorted(
                interactions['view_count'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
        }


class UserPreferences:
    def __init__(self):
        self.user_data = defaultdict(lambda: {
            'view_times': defaultdict(float),     # Paper ID -> total view time
            'view_counts': defaultdict(int),      # Paper ID -> view count
            'bookmarks': set(),                   # Set of paper IDs
            'downloads': set(),                   # Set of paper IDs
            'show_more': set(),                   # Set of paper IDs
            'show_less': set(),                   # Set of paper IDs
            'followed_authors': set(),            # Set of author IDs
            'blocked_authors': set(),             # Set of author IDs
            'last_interactions': defaultdict(datetime)  # Paper ID -> last view time
        })
    
    def record_view(self, user_id, paper_id, duration):
        if 0 < duration < 7200:  # Between 0 seconds and 2 hours
            self.user_data[user_id]['view_times'][paper_id] += duration
            self.user_data[user_id]['view_counts'][paper_id] += 1
            self.user_data[user_id]['last_interactions'][paper_id] = datetime.now()

    def toggle_bookmark(self, user_id, paper_id):
        if paper_id in self.user_data[user_id]['bookmarks']:
            self.user_data[user_id]['bookmarks'].remove(paper_id)
            return False
        self.user_data[user_id]['bookmarks'].add(paper_id)
        return True

    def toggle_download(self, user_id, paper_id):
        if paper_id in self.user_data[user_id]['downloads']:
            self.user_data[user_id]['downloads'].remove(paper_id)
            return False
        self.user_data[user_id]['downloads'].add(paper_id)
        return True

    def toggle_show_more(self, user_id, paper_id):
        if paper_id in self.user_data[user_id]['show_more']:
            self.user_data[user_id]['show_more'].remove(paper_id)
            return False
        self.user_data[user_id]['show_more'].add(paper_id)
        self.user_data[user_id]['show_less'].discard(paper_id)  # Remove from show_less if present
        return True

    def toggle_show_less(self, user_id, paper_id):
        if paper_id in self.user_data[user_id]['show_less']:
            self.user_data[user_id]['show_less'].remove(paper_id)
            return False
        self.user_data[user_id]['show_less'].add(paper_id)
        self.user_data[user_id]['show_more'].discard(paper_id)  # Remove from show_more if present
        return True

    def toggle_follow_author(self, user_id, author_id):
        if author_id in self.user_data[user_id]['followed_authors']:
            self.user_data[user_id]['followed_authors'].remove(author_id)
            return False
        self.user_data[user_id]['followed_authors'].add(author_id)
        self.user_data[user_id]['blocked_authors'].discard(author_id)  # Remove from blocked if present
        return True

    def toggle_block_author(self, user_id, author_id):
        if author_id in self.user_data[user_id]['blocked_authors']:
            self.user_data[user_id]['blocked_authors'].remove(author_id)
            return False
        self.user_data[user_id]['blocked_authors'].add(author_id)
        self.user_data[user_id]['followed_authors'].discard(author_id)  # Remove from followed if present
        return True