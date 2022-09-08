import Generic
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
import numpy as np
import json
from datetime import datetime
import csv

def main():
    Répertoire_1 = open("Répertoire_1.csv")
    Répertoire_2 = open()
    Répertoire_3 = open()
    reader = csv.reader("Répertoire_1.csv")
    csvwriter = csv.writer(Répertoire_2)
    csvwriter1 = csv.writer(Répertoire_3)
    files = []
    for file in reader:
        files.append(file)
        return files
    for e in files:
        if files[e] != []:
            csvwriter.writerow(e)
        else:
            csvwriter1.writerow(e)







