 caffeinate -dims uv run main2.py

scraped 2052 pages out of ~15K, got 43,318 datasets

for each page, the 20 dataset links were collected by looking for the "href" attribute and the "h3.dataset-heading a" CSS selector. 

| Field             | Attribute Type | Attribute Value                                                      |
|-------------------|-----------------|----------------------------------------------------------------------|
| title             | CSS_Selector     | h1[itemprop='name']                                                  |
| organization_type | CSS_Selector     | span.organization-type                                              |
| formats           | CSS_SELECTOR     | section#dataset-resources span.format-label                         |
| tags              | CSS_SELECTOR     | ul.tag-list li a                                                     |
| pubisher_heading  | CSS_SELECTOR     | section#organization-info h1.heading                                |
| publisher         | CSS_SELECTOR     | [title='publisher']                                                  |
| ~date_created~      | ~CSS_SELECTOR~     | ~span[itemprop='dateModified'] a~                                      |
| date_last_updated | XPATH            | //th[normalize-space(text())='{label_text}']/following-sibling::td   |

out of the 43,318 scraped, 16 datasets had no title. For 15/16 this is most likely due to some poor scraping in previous runs of the scraper which were not re-attempted due to the caching method implemented (checking to if the url existed in the dataset, which it always did if it was added to the csv in the first place). The 1 dataset with no title that had other fields populated was a magazine with no title from the 
EPA: https://catalog.data.gov/dataset/none-3b132. These were all dropped from the dataset as I wanted the title to be non nullable in the dataset.


3 of the dates were null, unsure why, perhaps some error in writing to file when I randomly canceled the script as they do have those fields populated on the site. Also, date_created was mistakenly populated using the `dateModified` field when it should have used `XPATH` and either the "Metadata Date" or the "Reference Date(s)", although none of these consistently give the date the datasource was initially published.
