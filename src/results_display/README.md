# Process

To visualize the results, a simple program has been written. It takes the raw results (text file, e.g. https://raw.githubusercontent.com/lukaselmer/ethz-web-scale-data-mining-project/master/src/results_display/example_input.txt) and generates some HTML and JavaScript in the file html/index.html. This file then can be opened in your favourite browser (tested only on Chrome) and the html can be saved as PDF. Be aware that opening the HTML file in the browser takes some time for rendering (approx 0.5-5 minutes, depending on the data complexity).

# Usage

```
# install ruby:
# apt-get install ruby
# yum install ruby

cd ethz-web-scale-data-mining-project/src/results_display
chmod +x display_result.rb
./display_result.rb example_input.txt
open html/index.html
```
