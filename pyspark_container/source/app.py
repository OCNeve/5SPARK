import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import re
import seaborn as sns

# Load the dataset (assuming the file is a CSV located at the given path)
@st.cache_data
def load_data():
    df = pd.read_csv("/home/jovyan/work/data/data.csv", header=None, encoding="ISO-8859-1")
    df.columns = ['target', 'ids', 'date', 'flag', 'user', 'text']
    df['date'] = df['date'].str.replace(r' [A-Z]{3}', '', regex=True)  # Remove timezone (e.g., 'PDT')
    df['date'] = pd.to_datetime(df['date'], format='%a %b %d %H:%M:%S %Y', errors='coerce')  # Parse the date
    return df

def random_sample(df, negative_ratio=0.5, positive_ratio=0.2):
    # Separate positive and negative samples
    negative_df = df[df['target'] == 0]
    positive_df = df[df['target'] == 4]
    
    # Sample with a different ratio for each sentiment group
    negative_sample = negative_df.sample(frac=negative_ratio, random_state=42)
    positive_sample = positive_df.sample(frac=positive_ratio, random_state=42)
    
    # Concatenate the samples back together
    sampled_df = pd.concat([negative_sample, positive_sample])
    return sampled_df

def extract_hashtags(text):
    return re.findall(r'#\w+', text)

# Display the dataset shape and the first few rows
st.title("Sentiment Analysis of Tweets/Toots")

# Load and cache the data
data = load_data()

# Add a checkbox to toggle between full dataset and random sample
use_random_sample = st.checkbox('Use random sample', key=1)

if use_random_sample:
    data = random_sample(data)
    st.write(f"Using a random sample of {data.shape[0]} rows.")
else:
    st.write(f"Using the full dataset of {data.shape[0]} rows.")

# Create tabs for each visualization
tabs = st.tabs(["Sentiment Distribution", "Toot Frequency", "Top Hashtags", "User Activity Heatmap"])

with tabs[0]:
    # Count the number of each sentiment
    sentiment_counts = data['target'].value_counts().sort_index()

    # Rename sentiment values (0: Negative, 2: Neutral, 4: Positive)
    sentiment_labels = {0: 'Negative', 4: 'Positive'}
    sentiment_counts.index = sentiment_counts.index.map(sentiment_labels)

    # Plotting the sentiment distribution
    st.subheader("Sentiment Distribution")
    fig, ax = plt.subplots()
    ax.bar(sentiment_counts.index, sentiment_counts.values, color=['red', 'green'])
    ax.set_xlabel('Sentiment')
    ax.set_ylabel('Count')
    ax.set_title('Distribution of Sentiments in the Dataset')

    st.write("This plot shows the distribution of tweet sentiments (positive, negative).")
    st.write("It is calculated by counting the occurrences of each sentiment label in the dataset.")

    # Display the bar chart in Streamlit
    st.pyplot(fig)

with tabs[1]:
    data = data.dropna(subset=['date'])

    # Group the data by date to count tweets/toots per day
    data_by_date = data.groupby(data['date'].dt.date).size()

    # Plotting the toot frequency over time
    st.subheader("Toot Frequency Over Time")
    fig, ax = plt.subplots()
    ax.plot(data_by_date.index, data_by_date.values, color='blue', marker='o')
    ax.set_xlabel('Date')
    ax.set_ylabel('Number of Toots')
    ax.set_title('Toot Frequency Over Time')

    # Rotate date labels for better readability
    plt.xticks(rotation=45)

    st.write("This plot shows the frequency of tweets over time.")
    st.write("It is calculated by counting the number of tweets for each date in the dataset.")
    
    # Display the line chart in Streamlit
    st.pyplot(fig)

with tabs[2]:
    data['hashtags'] = data['text'].apply(lambda x: extract_hashtags(x))
    all_hashtags = [hashtag for hashtags_list in data['hashtags'] for hashtag in hashtags_list]
    hashtag_counts = pd.Series(all_hashtags).value_counts().head(10)

    # Plotting the top hashtags as a horizontal bar chart
    st.subheader("Top Hashtags")
    fig, ax = plt.subplots()
    hashtag_counts.plot(kind='barh', ax=ax, color='blue')
    ax.set_xlabel('Frequency')
    ax.set_ylabel('Hashtag')
    ax.set_title('Top Hashtags in the Dataset')

    st.write("This plot shows the most frequently used hashtags in the tweets.")
    st.write("It is calculated by extracting hashtags from each tweet and counting their occurrences.")
    
    # Display the horizontal bar chart in Streamlit
    st.pyplot(fig)

with tabs[3]:
    # Create a new column for the hour of the tweet
    data['hour'] = data['date'].dt.hour

    # Limit to the top N users to reduce complexity (e.g., top 10 most active users)
    top_users = data['user'].value_counts().head(10).index
    filtered_data = data[data['user'].isin(top_users)]

    # Group by user and hour to get the count of toots
    user_activity = filtered_data.groupby(['user', 'hour']).size().unstack(fill_value=0)

    # Plotting the heatmap
    st.subheader("User Activity Heatmap")
    plt.figure(figsize=(12, 8))
    sns.heatmap(user_activity, cmap='YlGnBu', linewidths=.5, cbar_kws={'label': 'Number of Toots'})
    plt.title('User Activity (Toots per Hour)')
    plt.xlabel('Hour of Day')
    plt.ylabel('User')
    plt.xticks(rotation=0)  # Rotate x labels for better visibility

    st.write("This heatmap shows user activity by displaying the number of tweets per hour.")
    st.write("It is calculated by counting the number of tweets made by the top most active users, for each hour of the day.")

    # Display the heatmap in Streamlit
    st.pyplot(plt)
