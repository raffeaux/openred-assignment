# Openred Assignment

A small prototype I made for a job interview with OPENRED, a startup from Rotterdam.

Here the idea is to submit a CSV containing scraped housing data, validate it, clean 
it, extract features from it and eventually send it to a database so that it may be
queried for BI purposes or be used as a training set in an ML pipeline.

![Diagram][/diagram.png]

The app is currently running on CleverCloud, from where I can monitor and log any
errors. It also sends me webhooks every time I run the pipeline, letting me know
the main insights.