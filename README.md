# ETH Zurich - Web Scale Data Mining Project

This is the main repository for the web scale data mining project, which took place in summer 2014 as a research project.

## Directory Structure and Overview

<pre>
├── doc - the report of the project
└── src - the source code
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

### Source code projects

#### WSDA

The self-implemented LDA

TODO: add some more doc, references, etc.

@hany-abdelrahman: the WSDA directory should probably be renamed to something more meaningful :wink:

Author: [Hany Abdelrahman](https://github.com/hany-abdelrahman)

#### combine_sequence_files

Combines sequence files (http://wiki.apache.org/hadoop/SequenceFile) from subdirectories
into multiple sequence files. These sequence files have the same name as the subdirectories.

This way, it is possible to create a flat directory structure whith few large sequence files.

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### examples

Contains a spark example project and a simple word count application. Only for dev env setup purposes.

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### html_to_text_conversion


Author: [Lukas Elmer](https://github.com/lukaselmer)

#### remove_infrequent_words

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### results_display

A script to help displaying the topics. Generates

* A readable text version
* A tag cloud for each topic, each word size weighted by the probability of the word 

Author: [Lukas Elmer](https://github.com/lukaselmer)

#### scripts

#### word_count

Author: [Lukas Elmer](https://github.com/lukaselmer)


