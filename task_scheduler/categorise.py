import pandas as pd
from collections import defaultdict

def categorize_problem(problem_name):
    name_lower = problem_name.lower()
    categories = {
        'Array': ['array', 'subarray', 'element', 'sum', 'maximum', 'minimum', 'rotate', 'sort', 'merge', 'partition', 'rearrange', 'median', 'majority', 'duplicate', 'missing', 'intersection'],
        'String': ['string', 'substring', 'character', 'palindrome', 'anagram', 'pattern', 'match', 'replace', 'reverse', 'valid', 'decode', 'encode', 'compress', 'repeat'],
        'Linked List': ['linked list', 'node', 'cycle', 'intersection'],
        'Tree': ['tree', 'binary tree', 'bst', 'traversal', 'ancestor', 'depth', 'level order', 'preorder', 'inorder', 'postorder', 'zigzag', 'symmetric', 'balanced', 'diameter'],
        'Graph': ['graph', 'dfs', 'bfs', 'connected', 'component', 'path', 'route', 'island', 'clone', 'topological', 'cycle', 'shortest path'],
        'Dynamic Programming': ['climb', 'stairs', 'coin', 'house robber', 'edit distance', 'longest common', 'subsequence', 'knapsack', 'unique paths', 'decode ways', 'word break', 'palindrome partitioning'],
        'Hash Table': ['hash', 'map', 'frequency', 'count', 'group', 'anagram', 'duplicate', 'unique', 'occurrence'],
        'Two Pointers': ['two sum', 'three sum', 'four sum', '3sum', '4sum', 'container', 'trapping', 'remove duplicates', 'merge sorted'],
        'Binary Search': ['binary search', 'search', 'find', 'rotated', 'peak', 'sqrt', 'median', 'kth', 'smallest', 'largest'],
        'Stack/Queue': ['stack', 'queue', 'parentheses', 'bracket', 'valid', 'evaluate', 'calculator', 'next greater', 'monotonic', 'deque'],
        'Heap': ['heap', 'priority', 'kth largest', 'kth smallest', 'top k', 'median'],
        'Sorting': ['sort', 'merge', 'quick', 'bucket', 'radix', 'counting'],
        'Backtracking': ['permutation', 'combination', 'n-queens', 'sudoku', 'word search', 'generate', 'subsets'],
        'Greedy': ['greedy', 'interval', 'meeting', 'gas station', 'candy', 'jump game', 'activity'],
        'Bit Manipulation': ['bit', 'xor', 'binary', 'power of', 'single number', 'missing number', 'complement'],
        'Math': ['math', 'number', 'digit', 'integer', 'factorial', 'fibonacci', 'prime', 'gcd', 'lcm', 'roman', 'atoi'],
        'Sliding Window': ['window', 'sliding', 'subarray', 'substring', 'consecutive', 'longest', 'maximum', 'minimum'],
        'Design': ['design', 'implement', 'lru', 'cache', 'data structure', 'iterator', 'stream', 'logger', 'browser', 'underground'],
        'Matrix': ['matrix', '2d', 'grid', 'board', 'diagonal', 'spiral', 'rotate', 'zeros'],
        'Simulation': ['simulation', 'game', 'robot', 'walk', 'move', 'step', 'process']
    }

    category_scores = defaultdict(int)
    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in name_lower:
                if keyword in ['linked list', 'binary tree', 'binary search', 'dynamic programming']:
                    category_scores[category] += 3
                elif len(keyword) > 5:
                    category_scores[category] += 2
                else:
                    category_scores[category] += 1

    if 'two sum' in name_lower or '3sum' in name_lower or '4sum' in name_lower:
        category_scores['Two Pointers'] += 3
        category_scores['Hash Table'] += 2

    if 'tree' in name_lower and ('path' in name_lower or 'traverse' in name_lower):
        category_scores['Tree'] += 2

    if 'linked' in name_lower and 'list' in name_lower:
        category_scores['Linked List'] += 3

    if category_scores:
        return max(category_scores.items(), key=lambda x: x[1])[0]
    else:
        return 'Miscellaneous'

def categorize_excel(input_file, output_file):
    df = pd.read_excel(input_file)
    if 'problem_name' not in df.columns:
        raise ValueError("Excel file must have 'problem_name' column")

    df['Category'] = df['problem_name'].apply(categorize_problem)
    df.to_excel(output_file, index=False)
    print(f"Categorized problems saved to '{output_file}'")

if __name__ == "__main__":
    categorize_excel("leetcode_analysis_fixed.xlsx", "leetcode_analysis_categorized.xlsx")

