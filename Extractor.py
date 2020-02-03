import sys
import json

from time import time
from os import path, mkdir
from TweetPeeker import unwrap_line_to_dictionary, print_topics

"""
Concucts a simple semantic analysis on gathered tweets meeting language criteria (default is english),
checking most popular words appearing along with a keyword (topic) and hashtags.
Stores statistics of posting dates, languages of the posts and most active users regardint each topic.
The output is saved in json format.
Full list of options available with --help variable.
"""

class Extractor:
    """
    Extractor class that conducts analysis on gathered tweets and saves the output into a json file.\n
    """

    def __init__(self, topic, language):
        """
        Constructor of Extractor class.\n
        :param topic: the topic to anayze
        :param language: language of the tweets to be analyzed
        """
        self.topic = topic  # tweet keyword
        self.language = language  # analysis language
        self.dates = {}  # dates distribution
        self.followers = 0  # cumulative number of users following people that post about this topic
        self.languages = {}  # language distribution for this keyword
        self.hashtags = {}  # hashtags found in the tweets
        self.words = {}  # content analysis, words contained in tweets
        self.users = {}  # list of users which previously posted on this topic
        self.last_id = None  # limiter for continuous analyses (after getting new tweets only analyze the new ones)
        self.new_last_id = None  # this is going to be saved in output file

        self.tweets_count = 0  # how many tweets have been analyzed
        self.new_tweets_count = 0
        self.analysis_time = None  # start time of the analysis
        self.previous_10k_time = None

    def load_previous_analysis(self):
        """
        Loads content of previously conducted analysis for this specific topic and language.
        """
        try:
            with open('analyses/' + self.topic + '_' + self.language + '.json', 'r') as file:
                content = json.load(file)
            self.last_id = content['last_id']
            self.tweets_count = content['tweets_count']
            self.followers = content['followers']
            self.languages = content['languages']
            self.dates = content['dates']
            self.hashtags = content['hashtags']
            self.words = content['words']
            self.users = content['users']
        except FileNotFoundError:
            pass

    def save_the_analysis(self):
        """
        Sorting and saving the analysis output in a json file.
        """
        if not self.new_tweets_count:
            return
        if not path.exists('analyses'):
            mkdir('analyses')

        # sorting dates, languages and words    k-key, v-value
        self.languages = {k: v for k, v in sorted(self.languages.items(), key=lambda lang: lang[1], reverse=True)}
        self.dates = {k: v for k, v in sorted(self.dates.items(), key=lambda date: date[1], reverse=True)}
        self.words = {k: v for k, v in sorted(self.words.items(), key=lambda word: word[0], reverse=False)}
        self.words = {k: v for k, v in sorted(self.words.items(), key=lambda word: word[1], reverse=True)}
        self.hashtags = {k: v for k, v in sorted(self.hashtags.items(), key=lambda tag: tag[0], reverse=False)}
        self.hashtags = {k: v for k, v in sorted(self.hashtags.items(), key=lambda tag: tag[1], reverse=True)}
        self.users = {k: v for k, v in sorted(self.users.items(), key=lambda user: user[0], reverse=False)}
        self.users = {k: v for k, v in sorted(self.users.items(), key=lambda user: user[1], reverse=True)}

        trending = {}
        for k in list(self.hashtags)[:5]: trending[k] = self.hashtags[k]
        for k in list(self.words)[:10]: trending[k] = self.words[k]

        collection = {'last_id': self.new_last_id, 'tweets_count': self.tweets_count + self.new_tweets_count,
                      'tweets_applying_for_analysis': self.languages.get(self.language), 'followers': self.followers,
                      'languages': self.languages, 'dates': self.dates, 'trending': trending, 'hashtags': self.hashtags,
                      'words': self.words, 'users': self.users}
        with open('analyses/' + self.topic + '_' + self.language + '.json', 'w') as file:
            json.dump(collection, file, indent=3)
        print('Saved as \x1b[1;34;40m' + self.topic + '_' + self.language + '.json\x1b[0m\n')

    def analyze(self):
        """
        Analyzes the tweets for the topic.\n
        Counts followers, tweets themselves, checks tweets dates, and language they were written in.\n
        The biggest part is content analysis, that extracts and counts hashtags
        and counts all the distinct words that show up in analyzed tweets.
        """
        try:
            with open('outputs/' + self.topic + '.txt', 'r') as file:
                self.new_last_id = unwrap_line_to_dictionary(file.readline())['id']
                file.seek(0)

                start_time = time()
                for line in file:
                    if self.new_tweets_count % 10000 == 0:
                        if self.previous_10k_time:
                            print('\x1b[35m' + str(self.new_tweets_count//1000) + 'k time:',
                                  round((time()-self.previous_10k_time) * 1000, 3), 'ms.\x1b[0m')
                            self.previous_10k_time = time()
                        else:
                            self.previous_10k_time = time()

                    try:
                        line_content = unwrap_line_to_dictionary(line)
                        if line_content['id'] == self.last_id:
                            break

                        compare_name = line_content['screen_name'].strip('1234567890').lower()
                        if not ('iembot' in compare_name or compare_name[:3] == 'bot' or compare_name[-3:] == 'bot'):

                            # counting topic range
                            if line_content['screen_name'] not in self.users:
                                self.followers += int(line_content['user_followers'])
                                self.users[line_content['screen_name']] = 1
                            else:
                                self.users[line_content['screen_name']] += 1


                            # checking dates distribution
                            date = line_content['date'].split()[0]
                            if date in self.dates:
                                self.dates[date] += 1
                            else:
                                self.dates[date] = 1

                            # checking language dependency
                            if line_content['language'] in self.languages:
                                self.languages[line_content['language']] += 1
                            else:
                                self.languages[line_content['language']] = 1

                            # analyzing content
                            if line_content['language'] == self.language:
                                words = line_content['full_text'].replace(',', '').replace('.', '').replace('!', '')\
                                    .replace('?', '').replace('"', '').replace('\u2019', '\'').replace('\' ', ' ')\
                                    .replace(';', ' ').replace('\u2018', ' ').replace('*', ' ').replace(': ', ' ')\
                                    .replace(' (', ' ').replace(') ', ' ').replace(' -', ' ').replace(' i\'', ' I\'').split()
                                for word in words:
                                    if word.lower() not in self.topic and 'http' not in word:
                                        if len(word) > 1:
                                            if word[0] == '#':
                                                if word.lower() in self.hashtags:
                                                    self.hashtags[word.lower()] += 1
                                                else:
                                                    self.hashtags[word.lower()] = 1
                                            elif len(word) > 2 or (len(word) == 2 and word == word.upper()):
                                                if word[:-1] != word[:-1].upper():
                                                    word = word.lower()
                                                if word in self.words:
                                                    self.words[word] += 1
                                                else:
                                                    self.words[word] = 1
                            self.new_tweets_count += 1
                    except (IndexError, KeyError):
                        pass
                self.analysis_time = time() - start_time
                if self.new_tweets_count:
                    print('Analyzed \x1b[1;36;40m{}\x1b[0m tweets about \x1b[1;34;40m{}\x1b[0m in {} seconds.'.
                          format(self.new_tweets_count, self.topic, self.analysis_time))
                    print('Average time per tweet {} ms.'.format(self.analysis_time*1000 / self.new_tweets_count))
                else:
                    print('Found \x1b[1;36;40m0\x1b[0m new tweets about \x1b[1;34;40m' + self.topic + '\x1b[0m')
        except FileNotFoundError:
            print('Could not load tweets file.')

    def filter_words(self):
        """
        Filters the output off of words that are to generic. The word list is stored in assets/word_blacklist.txt\n
        """
        try:
            with open('assets/word_blacklist.txt', 'r') as file:
                blacklist = file.read().split()
            for word in blacklist:
                self.words.pop(word, None)
        except FileNotFoundError:
            pass


def analyze_topics(topic_list, language):
    """
    Provided list of topics and a language to conduct the analyze in,
    calls analyze_topic() function for every topic.\n
    If topic list is empty, it will load from the topics.txt file.\n
    If None passed as language, it will analyze in default which is english.\n
    :param topic_list: list of topics to perform analyze
    :param language: language of the posts to be content-analyzed
    """
    if not topic_list:
        topic_list = []
        try:
            with open('assets/topics.txt', 'r') as file:
                for line in file:
                    topic_list.append(line.strip())
        except FileNotFoundError:
            print('There is no topics file. Please pass a topic as a parameter.')
            exit()

    for topic in topic_list:
        if language:
            analyze_topic(topic, language)
        else:
            analyze_topic(topic)


def analyze_topic(topic, language='en'):
    """
    Performs analysis for specified topic in specified language or in english as default.\n
    :param topic: topic of the analysis
    :param language: language of the analysis
    """
    brain = Extractor(topic, language)
    brain.load_previous_analysis()

    brain.analyze()
    brain.filter_words()
    brain.save_the_analysis()


if __name__ == '__main__':
    topics = None
    language = None

    if len(sys.argv) > 1:
        for i in range(1, len(sys.argv)):
            sys.argv[i] = sys.argv[i].lower()

        if sys.argv[1][0] == '-':
            if sys.argv[1] == '--help' or sys.argv[1] == '-h':
                print('usage: python3 Extractor.py [-h] [-l en] [a b c...]\n'
                      '\n'
                      'analyze content for topics a, b, c...\n'
                      '\n'
                      'positional arguments:\n'
                      '  a, b, c...\t\t\t topics to analyze content for\n'
                      '\n'
                      'optional arguments:\n'
                      '  -h, --help\t\t\t show this help message and exit\n'
                      '  -t, --topics\t\t\t list followed topics\n'
                      '  -l, --language\t\t language for tweets analysis\n'
                      '\n'
                      'If no arguments passed, program will follow keywords loaded from topics.txt file.\n'
                      'Default analysis language is english.\n'
                      '\n'
                      'example usages:\n'
                      'python3 Extractor.py example\n'
                      'python3 Extractor.py -t\n'
                      'python3 Extractor.py --language en\n'
                      'python3 Extractor.py --language pt example topic\n')
                exit()
            elif sys.argv[1] == '-t' or sys.argv[1] == '--topics':
                print_topics()
                exit()
            elif sys.argv[1] == '-l' or sys.argv[1] == '--language':
                if len(sys.argv) == 2:
                    print('Pass 2 letters long language code in argument.')
                    exit()
                if len(sys.argv[2]) == 2:
                    language = sys.argv[2]
                    if len(sys.argv) > 3:
                        topics = [arg for arg in sys.argv[3:] if arg[0] != '-']
            else:
                print("Incorrect usage, for help use --help option.\n")
                exit()
        else:
            topics = [arg for arg in sys.argv[1:] if arg[0] != '-']

    analyze_topics(topics, language)

