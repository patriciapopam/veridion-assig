###

### 1. Pre-processing

Some lines contains commas within rows, making it difficult to correctly parse the files. Solved it by pre-processing the files with sed, replacing the ',' within quotations with ';'. Also replaced escape character. This fixes the data within the google dataset.

sed -E ':a; s/(\\[^*]*),([^*]*\\)/\1;\2/; ta' google_dataset.csv > google_preprocessed.csv

For the facebook dataset, removing all '\\"' occurences seems sufficient.

no. ROWS              | GOOGLE | FB    |   WEB |
--- | --- | --- | ---
ORIGINAL FILE         | 356520 | 72080 | 72018 |
BEFORE PRE-PROCESSING | 346925 | 71167 | 72018 |
AFTER PRE-PROCESSING  | 356520 | 72010 | 72018 |

### 2. Cleaning up

Name should be the most consistent data here, as all the other elements (address, website, phone number etc.) are more prone to change within the lifetime of the company. Some cleaning up of the data has been done, such as: removing trailing whitespaces, making all characters lowercase. Removing company suffixes (GmbH, .co, .ltd, SRL) proves to be more labor-intensive on an international dataset, as each country has its own suffixes [1], and has been skipped.

### 3. Pattern-matching

For matching the names a pattern matching algorithm [3] would be the way to go. The python library Fuzzywuzzy [2] implements the Levenshtein distance for approximate string matching within a given threshold. However, this is a computationally-intensive operation (hours of computation on the subset of US-based companies from the dataset), and has been omitted. In the following paragraphs, the discussed results will assume equality within the name fields of the three datasets.

### 4. Results

All name duplicates have been dropped for simplicity. An inner join on the facebook and google datasets provides a dataset of 34216 companies, which indicates that around half of the companies on the facebook list have a corespondent on the google one. With suffix-removal and pattern-matching, this number is expected to increase significantly.

An inner join on all three dataset provies a list of 3368 companies, representing less than 5% of the companies on the list. 

4439857
6853

## References:

1. https://en.wikipedia.org/wiki/List_of_legal_entity_types_by_country
2. https://pypi.org/project/fuzzywuzzy/
3. Levenshtein, Vladimir I. (February 1966). "Binary codes capable of correcting deletions, insertions, and reversals". Soviet Physics Doklady. 10 (8): 707–710.