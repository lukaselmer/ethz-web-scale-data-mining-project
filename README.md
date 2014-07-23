# ETH Zurich - Web Scale Data Processing and Mining Project

This is the main repository for the web scale data mining project, which took place in summer 2014 as a research project.

## Directory Structure and Overview

<pre>
└── src - the source code projects, see below
    ├── WSDA
    ├── combine_sequence_files
    ├── examples
    │   ├── spark_example
    │   └── word_count_1
    ├── html_to_text_conversion
    ├── remove_infrequent_words
    ├── results_display
    ├── scripts
    └── word_count
</pre>

### Repositories

* [This](https://github.com/lukaselmer/ethz-web-scale-data-mining-project) is the code repository
* The runs and the raw results can be found in [this repository](https://github.com/lukaselmer/ethz-web-scale-data-mining-project-runs)
* The hadoop config is [here](https://github.com/lukaselmer/ethz-web-scale-data-mining-project-hadoop-config)
* The spark config is [here](https://github.com/lukaselmer/ethz-web-scale-data-mining-project-hadoop-config)

### Source code projects

#### WSDA

The self-implemented LDA

@hany-abdelrahman: the WSDA directory should probably be renamed to something more meaningful :wink: TODO: add some more doc, references, etc.

Author: [Hany Abdelrahman](https://github.com/hany-abdelrahman)

#### combine_sequence_files

Combines [sequence files](http://wiki.apache.org/hadoop/SequenceFile) from subdirectories
into multiple sequence files. These sequence files have the same name as the subdirectories.

This way, it is possible to create a flat directory structure whith few large sequence files.

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### examples

Contains a spark example project and a simple word count application. Only for dev env setup purposes.

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### html_to_text_conversion

Converts [web archive records](https://en.wikipedia.org/wiki/Web_ARChive) into sequence files, removing all HTML / JS tags using [boilerplate](https://code.google.com/p/boilerpipe/) and doing some additional steps:

* remove stopwords
* remove words with non a-z characters
* try to remove non-english documents
* remove numbers
* remove URLs
* convert uppercase to lowercase charaters
* apply stemming

See also:
* [Specs](https://github.com/lukaselmer/ethz-web-scale-data-mining-project/blob/master/src/html_to_text_conversion/src/test/scala/TextProcessorSpec.scala)
* [TextProcessor](https://github.com/lukaselmer/ethz-web-scale-data-mining-project/blob/master/src/html_to_text_conversion/src/main/scala/TextProcessor.scala)

[Example how to use it](https://github.com/lukaselmer/ethz-web-scale-data-mining-project-runs/blob/master/convert_all9/convert_html.sh)

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### remove_infrequent_words

Removes words which appear infrequent. Needs a word count dictionary as input.

[Example how to use it](https://github.com/lukaselmer/ethz-web-scale-data-mining-project-runs/blob/master/remove_infrequent_words11/run.sh)

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### results_display

A script to help displaying the topics. Generates

* A readable text version
* A tag cloud for each topic, each word size weighted by the probability of the word 

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### scripts

@raynald: do you still need them? If no, please delete them :wink:

#### word_count

Simple word count for sequence files.

[Example how to use it](https://github.com/lukaselmer/ethz-web-scale-data-mining-project-runs/blob/master/word_count4/run.sh)

Author: [Lukas Elmer](https://github.com/lukaselmer)




