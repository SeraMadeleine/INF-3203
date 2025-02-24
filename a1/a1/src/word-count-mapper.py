# The 'map' function which will be executed in parallel.
# The input is a string of words separated by spaces.
# For word count, the output is a list of tuples where the first element is a word and the second element is the number 1.
# For example, the input "hello world" should return [('hello', 1), ('world', 1)].
# For use-cases other than word count, the logic should be different, but the input/output format and semantics should remain the same
def mapper(data):
    mapped_data = [(word, 1) for word in data.split()]
    return mapped_data

# The 'reduce' function which will be executed in parallel.
# The input is a list of tuples where the first element is a word and the second element is a number.
# The output is a list of tuples where the first element is a word and the second element is the total count of that word.
# For example, the input [('hello', 1), ('world', 1), ('hello', 1)] should return [('hello', 2), ('world', 1)].
# For other use-cases, the logic will be different, but the input/output format and semantics should remain the same
# Note for example that the partitioner logic expects to partition based on a key, so the output should be a list of tuples where the first element is the key.
# Also, the serializer expects the data to be in key/value pair format. The value can be any serializable data form, such as a list
# Thus, the mapper and reducer can use data with more than two elements, by using the first element as the key and the rest as the value 
def reducer(data):
    words = {}
    for entry in data:
        word, count = entry                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
        if word in words:
            words[word] += count
        else:
            words[word] = count
    return [[word, count] for word, count in words.items()]

