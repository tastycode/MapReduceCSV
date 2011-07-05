require 'quickl'
require 'csv'
# Apply map-reduce operations to flat files 
# 
# SYNOPSIS
#   #{program_name} [--help] [--version] [--config] [WHO]
# OPTIONS
#
# help::      Show help
# version::   Show version
# config::    Set config file
class MapReduceCSV  < Quickl::Command(__FILE__, __LINE__)
  attr_accessor :data, :mapped, :reduced

  VERSION = "0.1.0" 

  options do |opt|
    #show help 
    opt.on_tail("-?","--help","Show help") do
      raise Quickl::Help
    end

    #show verson
    opt.on_tail("-v","--version","Show version") do
      raise Quickl::Exit, "#{program_name} #{VERSION}"
    end

    opt.on("-c [CONFIG]","--config [CONFIG]", "Set config path") do |config_path|
      config(config_path)
    end
  end

  def initialize
    @data    = []
    @mapped  = {}
    @reduced = {}
  end

  # Load a CSV file
  #
  # ==== Parameters
  #
  # * +file+ - Path to a CSV file
  # * +file+ - Any options to pass to CSV 
  #
  # ==== Examples
  #
  #   mr = MasterCSV.new
  #   mr.load("path/to/file")
  #
  def load(file,options={})
    lines = CSV.read(file,options)
    header=lines.shift 
    @data = lines.collect do |line|
      #convert the line to a record hash
      record = Hash[*header.each_with_index.collect {|h,i| [h,line[i]] }.flatten]
    end
  end

  # Load a config file
  # 
  # Config files are defined in ruby and contain a module called MapReduceCSVConfig which defines 
  # methods map and reduce. The config file is included and the map/reduce methods become instance methods
  # of #MapReduceCSV 
  # 
  # ==== Parameters 
  # 
  # * +file+ - Path to a config file
  #
  # ==== Example Config
  #   #config for counting the records whose "age" is over 18
  #
  #   module MapReduceCSVConfig
  #     def map(record)
  #       emit('all',1) if record['age'].to_i > 18
  #     end
  #     def reduce(key,vals)
  #       vals.inject(&:+)
  #     end
  #   end
  def config(file)
    require file
    self.class.class_eval do 
      include MapReduceCSVConfig
    end
  end

  # A function called by the map function to store the map result under a key
  #
  def emit(key,value)
    @mapped[key]||=[]
    @mapped[key] << value
  end
  # Perform the map reduce operation 
  def exec
    @data.each do |record|
      map(record)
    end
   
    return @mapped unless respond_to? :reduce
      
    @reduced = {} 
    @mapped.keys.each do |key|
      @reduced[key]=reduce(key,@mapped[key])
    end
    @reduced 
  end

  def execute(args)
      file = args.last
      self.load(file)
      puts exec.inspect
      
  end
end

if __FILE__ == $0
  MapReduceCSV.run(ARGV,__FILE__)
end
