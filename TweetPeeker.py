import sys
import tweepy

from pandas import DataFrame
from numpy import array
from json import JSONDecodeError
from os import path, mkdir, rename, remove
from subprocess import check_output
from time import time, sleep
from subprocess import CalledProcessError


"""
This script queries Twitter API for tweets (used API is free but it only allows to access last 7 days).
It supports storing a topic list, re-fetching topics (meaning it wont fetch the same posts twice even if interrupted or crashed),
It saves the output in json-like format which was suitable for this purpose.
There is also an option to execute it along with Extractor.py script to concuct analysis on gathered posts.
Full list of options is available with --help variable.
"""

class TwitterFetcher:

    def __init__(self):
        """
        Initializes the fetcher object.
        """
        self.api = None  # api connection to twitter
        self.auth = None  # user authentication object
        self.query = None  # currently used keyword(s)
        self.max_id = None  # limits twitter queries -> pagination matters
        self.since_id = None  # takes care of not fetching tweets that are already saved in a file
        self.topics = []  # list of topics to fetch tweets for
        self.existing_topic = None  # flag saying if the file for current query already exists
        self.filters = ' -filter:retweets -filter:replies '  # twitter API filters for eliminating certain tweets
        self.request_counter = 0  # how many requests have been made since start of the script
        self.received_tweets = 0  # cumulative number of tweets received for a specific topic
        self.tweets_matching_keyword = 0  # how many of received tweets actually had keyword in their texts

        self.perform_analysis = False
        self.analysis_language = None
        self.retry_counter = 3  # initializing counter for retries on json decode error

    def authenticate(self, customer_token_path='tokens/ConsumerToken', customer_secret_path='tokens/ConsumerSecret'):
        """
        Authenticates twitter developer user. Needs access tokens.\n
        :param customer_token_path: relative path to customer token\n
        :param customer_secret_path: relative path to customer secret token\n
        :raise Exception: if either of the paths is incorrect.
        """
        try:
            with open(customer_token_path, 'r') as file:
                customer_token = file.readline().strip()
            with open(customer_secret_path, 'r') as file:
                customer_secret = file.readline().strip()
            self.auth = tweepy.AppAuthHandler(customer_token, customer_secret)
            self.api = tweepy.API(self.auth, wait_on_rate_limit=True)
        except FileNotFoundError:
            raise Exception('Could not find customer token/secret file.')

    def set_perform_analysis(self):
        """
        Sets self.perform_analysis variable to True
        """
        self.perform_analysis = True

    def update_query(self, query):
        """
        Updates current query for use in twitter queries.\n
        Resets bunch of variables.\n
        :param query: new query (keyword[s]) to be used
        """
        self.query = query
        self.max_id = None
        self.since_id = None
        self.received_tweets = 0
        self.tweets_matching_keyword = 0
        self.retry_counter = 3
        self.update_limit_id()

    def fetch_topics(self):
        """
        Fetches tweets for all topics contained in self.topics\n
        If self.perform_analysis flag set to True, analyzes every topic after fetching.
        """
        for topic in self.topics:
            self.update_query(topic)
            self.follow_topic()
            if self.perform_analysis:
                if not self.analysis_language:
                    analyze_topic(topic)
                else:
                    analyze_topic(topic, self.analysis_language)

    def follow_topic(self):
        """
        Requests all new tweets starting from just released ones.\n
        Runs until there are no more tweets returned by twitter.\n
        Prints some text and numbers to follow the progress.\n
        """
        while True:
            self.request_counter += 1
            tweets = self.get_tweets()
            if not tweets:
                if not self.max_id and not self.since_id:
                    print('This topic ({}) does not appear in twitter for 7 days.'.format(self.query))
                    return
                if not self.max_id and self.since_id and self.received_tweets == 0:
                    print('There are no new tweets about {}'.format(self.query))
                    return
                if self.existing_topic and not self.since_id:
                    self.update_limit_id(True)
                else:
                    print('Fetched {} tweets containing this keyword.'.format(self.received_tweets))
                    if self.tweets_matching_keyword == self.received_tweets:
                        print('\x1b[1;32;40m' + 'All of them contained the keyword: ' + self.query + '\x1b[0m')
                    elif self.tweets_matching_keyword >= .9 * self.received_tweets:
                        print('\x1b[1;36;40m' + str(self.tweets_matching_keyword) + ' contained the keyword: ' +
                              self.query + '   (90%> x >100%)' + '\x1b[0m')
                    elif self.tweets_matching_keyword >= .7 * self.received_tweets:
                        print('\x1b[1;34;40m' + str(self.tweets_matching_keyword) + ' contained the keyword: ' +
                              self.query + '   (70%> x >90%)' + '\x1b[0m')
                    elif self.tweets_matching_keyword >= .4 * self.received_tweets:
                        print('\x1b[1;33;40m' + str(self.tweets_matching_keyword) + ' contained the keyword: ' +
                              self.query + '   (40%> x >70%)' + '\x1b[0m')
                    else:
                        print('\x1b[1;31;40m' + str(self.tweets_matching_keyword) + ' contained the keyword: ' +
                              self.query + '   (0%> x >40%)' + '\x1b[0m')
                    if self.received_tweets > 0 and self.existing_topic:
                        self.merge_output_files()
                    return
            else:
                self.retry_counter = 3
                formatted = self.extract_data_to_json_format(tweets)
                self.append_to_file(formatted)

    def get_tweets(self):
        """
        Requests tweets in pack of 100 (maximum allowed) applying filters.\n
        :returns: tweets received from twitter requested for a keyword
        """
        if self.query is None:
            print('Query not set.')
            return

        try:
            if self.since_id:
                print('Requesting tweets containing:', self.query, '\t max_id =', self.max_id, '\t since_id =',
                                                       self.since_id, '\t(', self.request_counter, ')')

                tweets = self.api.search(q=self.query+self.filters, count=100, result_type='recent', max_id=self.max_id,
                                         since_id=self.since_id, tweet_mode='extended', include_entities=False)
            else:
                print('Requesting tweets containing:', self.query, '\t max_id =', self.max_id,
                      '\t(', self.request_counter, ')')

                tweets = self.api.search(q=self.query+self.filters, count=100, max_id=self.max_id,
                                         tweet_mode='extended', result_type='recent', include_entities=False)

        except tweepy.error.TweepError as error:
            if error.response.text == 'status code = 503':
                print('Server overloaded, waiting 5 sec...')
                sleep(5)
                self.request_counter += 1
                return self.get_tweets()
            else:
                print(error.response.text)
                exit()

        except JSONDecodeError:  # tweepy unhandled exception
            if self.retry_counter == 0:  # we dont want to make a deadlock, but a few tries may be helpful
                return
            print('\x1b[1;31;40mParsing error occured. Retrying.\x1b[0m\n')
            self.request_counter += 1
            self.retry_counter -= 1
            return self.get_tweets()

        if len(tweets) == 0:
            print('\x1b[1;31;40m' + 'Received tweets: ' + str(len(tweets)) + '\x1b[0m\n')
        elif len(tweets) < 50:
            print('\x1b[1;33;40m' + 'Received tweets: ' + str(len(tweets)) + '\x1b[0m\n')
        elif len(tweets) < 70:
            print('\x1b[1;34;40m' + 'Received tweets: ' + str(len(tweets)) + '\x1b[0m\n')
        else:
            print('\x1b[1;32;40m' + 'Received tweets: ' + str(len(tweets)) + '\x1b[0m\n')

        self.received_tweets += len(tweets)
        if tweets:
            self.max_id = int(tweets[-1].id)-1
        return self.filter_tweets_matching_keyword(tweets)  # returns only the ones that have the keyword in their text
        # return tweets

    def filter_tweets_matching_keyword(self, tweets):
        """
        Checks tweets for containing keyword in their text.\n
        :param tweets: tweets to check\n
        :return: tweets that meet the keyword criteria
        """
        matching = []
        for tweet in tweets:
            if self.query in tweet.full_text.lower():
                matching.append(tweet)
                self.tweets_matching_keyword += 1
        return matching

    def update_limit_id(self, since=False):
        """
        Loads query limiting id from file and stores it in object variable.\n
        :param since: Switches between limiter we want to load from file since_id/max_id
        """
        test_number = 0
        line = None
        while test_number < 3:  # tries 3 times, because sometimes first try was unsuccessful for some reason
            try:
                if since:
                    line = str(check_output(['head', '-1', 'outputs/'+self.query+'.txt'])).lstrip('b"{ ').rstrip(' }\\n\"')
                else:
                    line = str(check_output(['tail', '-1', 'outputs/'+self.query+'.txt'])).lstrip('b"{ ').rstrip(' }\\n\"')
            except CalledProcessError:
                if since:
                    print('Could not load since_id from a file (attempt {})'.format(test_number+1))
                else:
                    print('Could not load max_id from a file (attempt {})'.format(test_number+1))
            finally:
                test_number += 1

            if line is not None:
                if since:
                    self.since_id = int(unwrap_line_to_dictionary(line)['id'])  # sets since_id, its exclusive
                    if path.exists('outputs/' + self.query + '_head.txt'):
                        line = str(check_output(['tail', '-1', 'outputs/' + self.query + '_head.txt'])).lstrip(
                            'b"{ ').rstrip(' }\\n\"')
                        self.max_id = int(unwrap_line_to_dictionary(line)['id'])-1
                    else:
                        self.max_id = None  # this must be set to none, otherwise no tweets could be retrieved
                else:
                    self.max_id = int(unwrap_line_to_dictionary(line)['id'])-1  # sets max_id which is inclusive
                    self.existing_topic = True

    def extract_data_into_frame(self, tweets):
        """
        For extracting tweets into pandas data frame. (Used in one of the first versions abandoned due to data format)\n
        :param tweets: tweets to extract into frame\n
        :return: pandas data frame containing tweets info
        """
        frame = DataFrame()

        frame['id'] = array([tweet.id for tweet in tweets])
        frame['date'] = array([tweet.created_at for tweet in tweets])
        frame['user_location'] = array([tweet.user.location.replace('\n', ' ') for tweet in tweets])
        # if 'sc:' not in tweet.user.location and 'ig:' not in tweet.user.location and '#' not in tweet.user.location])
        frame['users_followers'] = array([tweet.user.followers_count for tweet in tweets])
        frame['retweet_count'] = array([tweet.retweet_count for tweet in tweets])
        frame['favorite_count'] = array([tweet.favorite_count for tweet in tweets])
        frame['language'] = array([tweet.lang for tweet in tweets])
        frame['full_text'] = array([tweet.full_text.replace('\n', ' ') for tweet in tweets])

        # frame = frame.reindex(index=frame.index[::-1])
        return frame

    def extract_data_to_json_format(self, tweets):
        """
        Extracts list of tweets into a json formatted dictionary.\n
        :param tweets: tweets to extract into dictionary
        :return: json-style dictionary containing tweets
        """
        json_style = {'tweets': []}

        for tweet in tweets:  # [::-1]:
            json_style['tweets'].append({
                'id': tweet.id,
                'date': tweet.created_at,
                'screen_name': tweet.user.screen_name,
                'user_location': tweet.user.location.replace('\n', ' '),
                'user_followers': tweet.user.followers_count,
                'retweet_count': tweet.retweet_count,
                'favorite_count': tweet.favorite_count,
                'language': tweet.lang,
                'full_text': tweet.full_text.replace('\n', ' ')  # for some reason some tweets still break the line
            })

        return json_style

    def append_to_file(self, data):
        """
        Saves tweets at the end of a respective file.\n
        :param data: json-formatted tweets dictionary
        """
        if not path.exists('outputs'):
            mkdir('outputs')

        with open('outputs/' + self.query + '_head.txt' if self.since_id else
                  'outputs/' + self.query + '.txt', 'a') as output:
            for tweet in data['tweets']:
                buffer = '{ '
                key_number = 1
                for key in tweet:
                    buffer += '\'' + str(key) + '\':' + '\'' + str(tweet[key]) + '\''
                    if key_number < len(tweet.keys()):
                        buffer += ', '
                    else:
                        buffer += ' }\n'
                    key_number += 1
                output.write(buffer)

    def merge_output_files(self):
        """
        Merges two files containing tweets of the same topic.\n
        Reads topic file line by line and attaches it to topic_head, then removes topic file and changes head's name.\n
        """
        if self.tweets_matching_keyword:
            try:
                with open('outputs/' + self.query + '.txt', 'r') as input_handle:
                    t = time() * 1000
                    with open('outputs/' + self.query + '_head.txt', 'a') as output_handle:
                        for line in input_handle:
                            output_handle.write(line)
                        print('Merged output files in \x1b[1;36;40m{} ms\x1b[0m.\n'.format(time() * 1000 - t))
                        output_handle.close()
                    input_handle.close()
                remove('outputs/' + self.query + '.txt')
                rename('outputs/' + self.query + '_head.txt', 'outputs/' + self.query + '.txt')
            except FileNotFoundError:
                print('Could not open desired file')

    def save_statistics(self):  # subject to development
        """
        Gathers some overal statistics about gathered tweets.\n
        :return:
        """
        if not path.exists('statistics'):
            mkdir('statistics')

        with open('statistics/keyword_match.txt', 'a') as file:
            file.write(str(self.tweets_matching_keyword) + ' / ' + str(self.received_tweets) + '\n')


def unwrap_line_to_dictionary(line):
    '''
    Gets a line containing tweet data and returns it in a form of a dictionary\n
    :param line: line to turn into a dictionary
    :return: dictionary form of passed line (tweet)
    '''
    # preprocessing done for posts containing i.e. quotes
    if line[0] == '\'':
        line = line.strip('\'').rstrip('\\n')
    if line[2] == '\\':
        line = line.replace('\\', '')

    line = line.lstrip('{ \'').rstrip('\' }')
    d = dict()
    for pair in line.split('\', \''):
        pair = pair.split('\':\'')
        d[pair[0]] = pair[1]
    return d


def display_help():
    """
    Displays help message for script usage.\n
    """
    print('usage: python3 TweetPeeker.py [-h][-t]\n'
          '       python3 TweetPeeker.py [-r] [-d][-a] a b c ...\n'
          '\n'
          'fetch tweets about topics a, b, c...\n'
          '\n'
          'positional arguments:\n'
          '  a, b, c...\t\t\t keywords to follow (added to list in topics.txt file)\n'
          '\n'
          'optional arguments:\n'
          '  -a, --analyze\t\t\t performs analysis for the topics after fetching\n'
          '  -d, --dry-run [a,b...]\t runs without saving passed topics to the file\n'
          '  -h, --help\t\t\t show this help message and exit\n'
          '  -r, --remove [a,b...]\t\t remove keywords from topic list\n'
          '  -t, --topics\t\t\t list followed topics\n'
          '\n'
          'If no arguments passed, program will follow keywords loaded from topics.txt file '
          'if no such file exists, it will ask you for a keyword to follow, '
          'create the file and save the keyword.\n'
          'If performing analysis, a language can be specified with 2 letter descriptor.'
          'If not specified, analysis will be conducted for posts in english\n'
          '\n'
          'example usages:\n'
          'python3 TweetPeeker.py example\n'
          'python3 TweetPeeker.py -t\n'
          'python3 TweetPeeker.py -a pt\n'
          'python3 TweetPeeker.py --remove example \'another topic\'\n'
          'python3 TweetPeeker.py -da example\n'
          'python3 TweetPeeker.py -a ge -d \'language specified\'\n'
          'python3 TweetPeeker.py -a --dry-run topic1 topic2 \'another topic\'\n')


def print_topics():
    """
    Prints followed topics, saves in topics.txt file in assets directory.
    """
    topics = []
    try:
        with open('assets/topics.txt', 'r') as file_handle:
            for line in file_handle:
                topics.append(line.strip())
        if topics:
            for topic in topics:
                print(topic)
        else:
            print('Topic list is empty.')
    except FileNotFoundError:
        print('Topic list doesn\'t exist.')


def remove_topics():
    """
    Removes topics passed in script execution arguments from topics.txt file.
    """
    if len(sys.argv) == 2:
        print('Correct usage: python3 tweetLookup.py {} topic'.format(sys.argv[1]))
    else:
        topics = []
        try:
            with open('assets/topics.txt', 'r') as file_handle:
                for line in file_handle:
                    topics.append(line.strip())
            with open('assets/topics.txt', 'w') as file_handle:
                for topic in topics:
                    if topic not in sys.argv[2:]:
                        file_handle.write(topic + '\n')
        except FileNotFoundError:
            print('Topic list does not exist.')


if __name__ == '__main__':
    lurk = TwitterFetcher()

    dry_run = True if ('-d' in sys.argv or '--dry-run' in sys.argv or
                       '-ad' in sys.argv or '-da' in sys.argv) else None
    analyze = True if ('-a' in sys.argv or '--analyze' in sys.argv or
                       '-ad' in sys.argv or '-da' in sys.argv) else False

    if len(sys.argv) == 1:  # when executed without arguments
        try:
            with open('assets/topics.txt', 'r') as handle:  # tries to load topics from file
                for line in handle:
                    lurk.topics.append(line.strip())
        except FileNotFoundError:  # on error asks user to input a topic from console
            new_topic = input('What keyword(s) would you like to follow?\n')
            if not path.exists('assets'):
                mkdir('assets')
            with open('assets/topics.txt', 'a') as handle:  # saves the topic to a file
                handle.write(new_topic + '\n')
            lurk.topics.append(new_topic)
    else:
        # lowering all of the arguments to always save to the same files
        for i in range(1, len(sys.argv)):
            sys.argv[i] = sys.argv[i].lower()

        if analyze:
            arg = [arg for arg in sys.argv if arg in ['-a', '-da', '-ad', '--analyze']]  # what was the trigger
            if len(arg) > 1:  # obvious incorrect usage
                print("Incorrect usage, for help use --help option.\n")
                exit()
            index = sys.argv.index(arg[0])  # index of the argument that triggered analysis
            if len(sys.argv) > index+1:  # check if there is anything behind the trigger
                if len(sys.argv[index+1]) == 2:  # check if it (probably) is a language code
                    lurk.analysis_language = sys.argv[index+1]
            from Extractor import analyze_topic

        if sys.argv[1][0] == '-':  # checks for dash, dashed arguments must be provided before anything else
            if sys.argv[1] == '--help' or sys.argv[1] == '-h':
                display_help()
                exit()
            if sys.argv[1] == '-t' or sys.argv[1] == '--topics':
                print_topics()
                exit()
            if sys.argv[1] == '--remove' or sys.argv[1] == '-r':
                remove_topics()
                exit()
            if sys.argv[1] in ['-a', '-da', '-ad', '-d', '--analyze', '--dry-run']:
                pass
            else:
                print("Incorrect usage, for help use --help option.\n")
                exit()

        passed_topics = []
        for i in range(1, len(sys.argv)):
            if sys.argv[i][0] != '-':  # only topics, no arguments
                if len(sys.argv[i]) > 2 or not analyze or i != index+1:  # if analysis triggered, skip the language code
                    passed_topics.append(sys.argv[i])

        if not dry_run:
            existing_topics = []
            try:
                with open('assets/topics.txt', 'r') as source:  # loads previously existing topics
                    for line in source:
                        existing_topics.append(line.strip())
            except FileNotFoundError:
                existing_topics = None
            with open('assets/topics.txt', 'a') as handle:
                for topic in passed_topics:
                    if existing_topics is not None and topic not in existing_topics:  # checks for repeated topics
                        handle.write(topic + '\n')  # saves topics to a file
                    if topic not in lurk.topics:  # checks for repeated topics
                        lurk.topics.append(topic)  # adds topics to current query list
        else:
            for topic in passed_topics:
                lurk.topics.append(topic)

    if analyze:
        lurk.set_perform_analysis()

    lurk.authenticate()
    lurk.fetch_topics()
