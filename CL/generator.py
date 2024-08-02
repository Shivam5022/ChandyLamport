import os
import pandas as pd
import random

def generate_random_sentence(word_list, num_words=10):
    return ' '.join(random.choice(word_list) for _ in range(num_words))

def generate_csv_files(folder_path, n, file_size_mb, num_words_per_row=10):
    # Ensure the directory exists
    os.makedirs(folder_path, exist_ok=True)

    # List of common English words for generating sentences
    word_list = [
        'the', 'quick', 'brown', 'fox', 'jumps', 'over', 'lazy', 'dog',
        'hello', 'world', 'python', 'code', 'generate', 'csv', 'file',
        'random', 'sentence', 'data', 'test', 'example', 'text', 'string'
        # Add more words as needed
    ]

    # Define the number of rows needed for each CSV file
    # Approximate size per row: 1 KB, so for 3 MB, we need around 3000 rows
    rows_per_file = int(file_size_mb * 1024 / 1)  # Adjust the row size estimate as needed

    for i in range(n):
        # Generate random English sentences
        data = {
            'text': [generate_random_sentence(word_list, num_words_per_row) for _ in range(rows_per_file)]
        }
        df = pd.DataFrame(data)

        # Define file name
        filename = os.path.join(folder_path, f'file_{i+1}.csv')

        # Save DataFrame to CSV
        df.to_csv(filename, index=False)

        print(f'Generated {filename} with approximately {file_size_mb} MB.')

# Example usage
folder_path = 'csv_files'
n = 20  # Number of CSV files to generate
file_size_mb = 1  # Size of each file in MB

generate_csv_files(folder_path, n, file_size_mb)
