# Databricks notebook source

from general.base import *

# COMMAND ----------

def generate_random_str():
    
    def get_random_string(length):
        # choose from all lowercase letter
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def random_letters():
        return random.choice(string.ascii_letters)

    def trim_by_x(inputstr = None, x = None):
        return inputstr[0:x]

    letters = ''
    for i in range(random.randint(0,100)):
        letter = random_letters()
        letters += letter

    data = get_random_string(random.randint(1,25))
    data += letters
    data = trim_by_x(data, random.randint(0,50))
    return data

# COMMAND ----------

while True:
    data = generate_random_str()
    print(data)

# COMMAND ----------


