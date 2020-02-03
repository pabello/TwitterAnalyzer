import sys
import json

from glob import glob
from os import path, mkdir
from datetime import datetime
from matplotlib import colors, pyplot as plt


"""
Plots charts based on data extracted by Extractor.py.
For now these are 3 plots: date dependency, 5 most popular hashtags, 10 mostly used words.
Implementation of this script is dependent on data analysis made by Extractor.py.
"""

class Plotter:

    def __init__(self, topic, transparency):
        self.topic = topic
        self.transparency = transparency

        self.name = None
        self.dates = {}
        self.hashtags = {}
        self.words = {}

        self.paths = None

    def work(self):
        self.load_file_paths()

        if not self.paths:
            print('Did not find statistics for keyword \x1b[1;40;31m{}\x1b[0m.'.format(self.topic))
            return

        for path in self.paths:
            try:
                self.load_data(path)
                self.name = path.replace('analyses/', '').replace('.json', '')
                self.plot()
                print('Generated charts for keyword \x1b[1;40;32m{}\x1b[0m.'.format(self.topic))
            except FileNotFoundError:
                print('Could not open {} file, proceeding.'.format(path))

    def load_file_paths(self):
        self.paths = [dir for dir in glob('analyses/' + self.topic + '_*') if self.topic in dir]

    def load_data(self, path):
        with open(path, 'r') as file:
            content = json.load(file)
            self.dates = content['dates']
            self.hashtags = {k: content['trending'][k] for k in content['trending'] if k[0] == '#'}
            self.words = {k: content['trending'][k] for k in content['trending'] if k[0] != '#'}

    def plot(self):
        self.dates = {k: v for k, v in sorted(self.dates.items(), key=lambda date:date[0])}
        # fig, dates_step = plt.subplots()
        # dates_step.step(list(self.dates.keys())[1:-1], list(self.dates.values())[1:-1], where='mid')
        # dates_step.plot(list(self.dates.keys())[1:-1], list(self.dates.values())[1:-1], 'C0o', alpha=0.5)
        # for label in dates_step.get_xticklabels():
        #     label.set_rotation(90)
        # dates_step.set_ylabel('Tweets containing ' + self.topic + ' keyword.')

        # bar chart showing dates distribution
        dates_labels = list(self.dates.keys())[1:-1]
        dates_values = list(self.dates.values())[1:-1]
        dates_fig, dates_bar = plt.subplots()
        fracs = [n / max(dates_values) for n in dates_values]
        colors = [(152*n/255, (55+200*n)/255, (100-100*n)/255) for n in fracs]
        dates_bar.bar(dates_labels, dates_values, color=colors)
        for label in dates_bar.get_xticklabels():
            label.set_rotation(90)

        # pie chart showing 5 most popular tags
        tags_fig, tags_pie = plt.subplots()
        tags_labels = [k + '\n' + str(v) for k, v in self.hashtags.items()][::-1]
        tags_values = list(self.hashtags.values())[::-1]
        explode = (.0, .0, .0, .0, .085)
        colors = ['#99add6', '#738fc7', '#4d70b8', '#2652a8', '#003399']
        tags_pie.pie(tags_values, labels=tags_labels, colors=colors, explode=explode, startangle=90)

        # bar chart illustrating 10 most often used words
        words_fig, words_bars = plt.subplots()
        words = list(self.words.keys())
        counts = list(self.words.values())
        colors = ['#660066', '#751466', '#852966', '#943d66', '#a35266', '#b26666', '#c27a66', '#d18f66', '#e0a366', '#f0b866']
        words_bars.barh(words, counts, color=colors)

        if not path.exists('plots'):
            mkdir('plots')
        if not path.exists('plots/' + self.topic):
            mkdir('plots/' + self.topic)

        dates_fig.savefig('plots/' + self.topic + '/' + self.name + '_dates_' +
                          datetime.now().strftime('%Y%m%d_%H%M%S') + '.png',
                          bbox_inches='tight', transparent=self.transparency)
        tags_fig.savefig('plots/' + self.topic + '/' + self.name + '_hashtags_' +
                         datetime.now().strftime('%Y%m%d_%H%M%S') + '.png',
                         bbox_inches='tight', transparent=self.transparency)
        words_fig.savefig('plots/' + self.topic + '/' + self.name + '_words_' +
                          datetime.now().strftime('%Y%m%d_%H%M%S') + '.png',
                          bbox_inches='tight', transparent=self.transparency)

        plt.close('all')


if __name__ == '__main__':
    transparency = False
    if len(sys.argv) > 1:
        if sys.argv[1][0] == '-':
            if sys.argv[1] == '-t' or sys.argv[1] == '--transparent':
                transparency = True
        topics = [arg for arg in sys.argv[1:] if arg[0] != '-']
    else:
        try:
            with open('assets/topics.txt') as file:
                topics = [line.strip() for line in file]
        except FileNotFoundError:
            print('No topics to plot. Pass them in argument or save in assets/topics.txt file')
            exit()

    for topic in topics:
        plotter = Plotter(topic, transparency)
        plotter.work()
