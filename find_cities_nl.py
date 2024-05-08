import wikipediaapi
wiki_wiki = wikipediaapi.Wikipedia('WikiScraper', 'nl')

page_py = wiki_wiki.page('Lijst van Nederlandse plaatsen')

def save_cities(page):
    links = page.links
    with open('steden.txt', 'w') as file:
        for title in sorted(links.keys()):
            if '(' in title:
                index = title.index('(')
                title = title[:index]
            file.write(title.strip())
            file.write('\n')
save_cities(page_py)