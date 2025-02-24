import sys
# The 'map' function which will be executed in parallel.
# The input is a string of words separated by spaces.
# For word count, the output is a list of three variables where the first element is the page, the second element is the page's rank OR the list of outgoing links

def mapper(data):
    output = []  # Store key-value pairs

    # Iterate through each line of the data (one line is one page and its utgoing links)
    for line in data.strip().split("\n"):

        # Skip emty lines 
        if not line:
            continue

        pages = line.split(", ")        
        mainpage = pages[0]             # First item is the mainpage
        outgoing_links = pages[1:] if len(pages) > 1 else []  # List of outgoing pages

        rank = 1.0  #innitially PR=1.0
        num_links = len(outgoing_links)

        # append the links
        output.append((mainpage, f"links:{','.join(outgoing_links)}"))
        
        # If there are outgoing links, distribute PR 
        if num_links > 0:
            contribution = rank / num_links
            for link in outgoing_links:
                output.append((link, contribution)) 


    return output  # Return a list of (key, value) pairs where the value can be a rank or a list




# The 'reduce' function which will be executed in parallel.
# The input is a list of tuples where the first element is a page and the second element is the rank OR the number of links.
# the input might consists of multiple page ranks for the same page, its the reducers job to reduce :))))
# The output is a list of tuples where the first element is a page and the second element is the page's new rank.
# the output only has unique pages 

# TODO: må ta vare på links her også?? 

def reducer(data):
    pages = {}      # store PageRank values 
    links = {}      # store graph structures (outgoing links)

    for entry in data:
        page, rank = entry

        # If the value starts with "links:", it's a structure preservation entry
        if isinstance(rank, str) and rank.startswith("links:"):
            links[page] = rank
        else:  # Otherwise, it's a PageRank contribution
            if page in pages:
                pages[page] += rank
            else:
                pages[page] = rank

    output = []

    # Make sure all nodes appear in the final output 
    all_nodes = set(pages.keys()).union(set(links.keys()))      # The union of all known nodes 
    
    # add a list of (key, value) pairs to the output list 
    for page in all_nodes:
        # Set the default value to 0.0 if there is no contribution 
        pr_value = pages.get(page, 0.0)
        # If there are any links left, include them, if nor, return the pagerank 
        if page in links:
            output.append([page, pr_value, links[page]]) 
        else:
            output.append([page, pr_value]) 
    
    return output