module MapReduceCSVConfig
  def map(record)
    emit("count",1)
  end
  def reduce(key,vals)
    vals.inject(0) do |o,val|
      o+=val
      o
    end
  end
end
