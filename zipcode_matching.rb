#!/usr/bin/env ruby

require "oj"
require "digest/md5"
require "base64"
require 'mongo'
require 'pp'

include Mongo

host = ENV['MONGO_RUBY_DRIVER_HOST'] || 'places-mongo-1.placed.com'
port = ENV['MONGO_RUBY_DRIVER_PORT'] || '30005'

puts "Connecting to #{host}:#{port}"
db = MongoClient.new(host, port).db('zip4')
coll = db.collection('zip4')

# Count.
puts "There are #{coll.count} records."

digest = Digest::MD5.new

last_zip4 = ""
counter = 0
match = 0
STDIN.each_line do |line|
  next if line.strip.start_with?("200708Copyright(C)USPS")
  o = {}
  o[:zip4] = line[0,9].strip
  next if o[:zip4] == last_zip4
  last_zip4 = o[:zip4]

  f_lat = line[39,9].to_f/1000000
  f_long = line[48,10].to_f/1000000
  t_lat = line[58,9].to_f/1000000
  t_long = line[67,10].to_f/1000000
  o[:bbox] = [f_long, f_lat, t_long, t_lat]

  centroid = [
    (t_long-f_long)/2+f_long,
    (t_lat-f_lat)/2+f_lat
  ]

# Find all records. find() returns a Cursor.
cursor = coll.find("loc"=>centroid)
if cursor.count != 0
	match+=1
	puts "matched"
else
	puts "not found"
end
counter+=1

# Print them. Note that all records have an _id automatically added by the
# database. See pk.rb for an example of how to use a primary key factory to
# generate your own values for _id.
#puts "Print each document individually:"
#pp cursor.each { |row| pp row }

end
puts "There are #{counter} records, #{match} matched."

############# RESULT #################
# ruby mongo_server.rb < 01_al/001.txt 
# There are 2880 records, 1588 matched.

