#! /usr/bin/ruby

class Term
  attr_accessor :word, :probability

  def initialize(s)
    @word, @probability = s.split(':')
    @probability = @probability.to_f
  end
end

class Topic
  attr_accessor :id, :terms_by_probability, :terms_by_name

  def initialize(str)
    # str looks like:
    # 9\t{car:0.0065967561177527915,citi:0.005459655518901955,music:0.004991794173514252,auto:0.004858515080998506,hotel:0.00464211954073639,park:0.004147248424495917,room:0.0037326825546470047,year:0.0032589358881492114,post:0.0032061313832683546,locat:0.003178411029853716}
    @id, words = str.split("\t")
    @terms_by_probability = words.gsub('{', '').gsub('}', '').split(',').collect { |s| Term.new(s) }
    @terms_by_name = @terms_by_probability.sort_by { |t| t.word }
  end

  def to_s
    out = []
    out << "Topic #{@terms_by_name.collect { |t| t.word }.join(', ')}, id: #{@id}"
    @terms_by_probability.each do |t|
      out << " * #{t.word} : #{t.probability}"
    end
    out << "\n"
    out.join("\n")
  end
end

if ARGV.length == 0
  puts "Usage: display_results <textfile>"
  exit(1)
end


topics = []

open(ARGV[0]).each do |line|
  topics << Topic.new(line)
end

topics.sort_by! { |topic| topic.terms_by_name.first.word }
topics.each { |t| puts t.to_s }
