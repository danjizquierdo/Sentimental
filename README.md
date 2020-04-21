# Sentimental
Toolset for sentiment and statistical analysis of twitter data

## Data Analyzed
The data used for this analysis was streamed from Twitter starting on February 24th and leading up to March 3rd (Super Tuesday). Tweets were gathered which fulfilled the following requirements:
* text contained the name of one of the candidates
* tweet mentioned one of the candidates
* tweet was sent or retweeted by one of the candidates

The tweets were stored in a network database (Neo4J), processed/cleaned with RegEx and NLTK (Python’s Natural Language Toolkit) and then visualized using Seaborn and Matplotlib. The sentiment of each tweet was calculated with VADER (Valence Aware Dictionary sEntiment Reasoner). In total 753 tweets from the candidates and ~94,000 tweets about the candidates were looked at.

## Results
An analysis of the results can be found at: https://towardsdatascience.com/super-tuesday-getting-sentimental-303a8ecc0212

Some visualization highlights are shown below:

![Swarm plot of sentiment around candidates](https://github.com/danjizquierdo/Sentimental/blob/master/images/sent_swarm_2.png?raw=true)
![Polar plot of sentiment by time of day for each candidate](https://github.com/danjizquierdo/Sentimental/blob/master/images/sent_polar_candid.png?raw=true)
![Top words about Joe Biden](https://github.com/danjizquierdo/Sentimental/blob/master/images/com_biden.png)
