require 'rubygems'
require 'csv'
require File.dirname(__FILE__)+"/../lib/mrcsv.rb"
describe MapReduceCSV do
  before(:each) do
    csv_rows = [['name','age','score'],
      ['joe',29,30.3],
      ['jane',35,29.1],
      ['chris',19,50.3],
      ['kristy',40,21.1]
    ]
    @test_csv_path="tmp_mrcsv_spec.csv"
    @config_path=File.dirname(__FILE__)+"/mrcsv_spec_config.rb"
    CSV.open(@test_csv_path,"w") do |csv|
      csv_rows.each do |row|
        csv << row
      end
    end
  end
  it "should be able to open a csv file" do
    mr=MapReduceCSV.new
    mr.load(@test_csv_path)
  end
  context "with a CSV file" do
    before(:each) do
      @mr=MapReduceCSV.new
      @mr.load(@test_csv_path)
    end
    it "should be able to open a ruby map reduce config file" do
      @mr.config(@config_path)
      @mr.methods.should include(:map)
      @mr.methods.should include(:reduce)
    end
    context "with a basic map config" do
      it "should be able to map" do
        MapReduceCSV.class_eval do 
          def map(record)
           emit(nil,1) 
          end
        end
        @mr.exec[nil].should have(4).items
      end
      it "should be able to map and reduce" do
        MapReduceCSV.class_eval do
          def map(record)
            emit(nil,1)
          end
          def reduce(key,vals)
            vals.inject(&:+)
          end
        end
        @mr.exec[nil].should == 4
      end
      it "should be able to group data using map reduce" do
        MapReduceCSV.class_eval do
          def map(record)
            emit(record['name'][/s/] ? 'sneaky' : 'non-sneaky', record['score'].to_f)
          end
          def reduce(key,vals)
            vals.inject(&:+)
          end
        end
        #sneaky and non-sneaky is determined by the presence of an S in the name. Score is mapped and summed
        @mr.exec['sneaky'].should == 71.4
      end
    end
  end

end
