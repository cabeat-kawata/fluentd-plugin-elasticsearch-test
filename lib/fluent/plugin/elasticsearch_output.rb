# encoding: UTF-8
require "date"
require "patron"
require "elasticsearch"
require "digest/sha2"

class Fluent::ElasticsearchOutput < Fluent::BufferedOutput

  Fluent::Plugin.register_output("elasticsearch", self)

  config_param :host, :string,  :default => "localhost"
  config_param :port, :integer, :default => 9200
  config_param :logstash_prefix, :string, :default => "logstash"
  config_param :logstash_dateformat, :string, :default => "%Y.%m.%d"
  config_param :type_name, :string, :default => "tenma-deliver"
  config_param :index_name, :string, :default => "tenma-deliver"
  config_param :log_type, :string, :default => nil
  config_param :hosts, :string, :default => nil

  include Fluent::SetTagKeyMixin

  config_set_default :include_tag_key, false

  def initialize
    super
  end

  def configure(conf)
    super
  end

  def start
    super
  end

  def client
    @_es ||= Elasticsearch::Client.new :hosts => get_hosts, :reload_connections => true, :adapter => :patron, :retry_on_failure => 5
    raise "Can not reach Elasticsearch cluster (#{@host}:#{@port})!" unless @_es.ping
    @_es
  end

  def get_hosts
    if @hosts
      @hosts.split(",").map {|x| x.strip}.compact
    else
      ["#{@host}:#{@port}"]
    end
  end

  def format(tag, time, record)
    [tag, record].to_msgpack
  end

  def shutdown
    super
  end

  def write(chunk)
    bulk_message = []

    chunk.msgpack_each do |tag, record|
      add_bulk_message(bulk_message,record)
    end

    send(bulk_message) unless bulk_message.empty?
    bulk_message.clear
  end

  def add_bulk_message(bulk_message,record)
    case @log_type
      when "bid" then
        add_bid_bulk_message(bulk_message,record)
      when "wn" then
        puts("TODO wn")
      when "imp" then
        puts("TODO imp")
      when "rd" then
        puts("TODO rd")
      when "cv" then
        puts("TODO cv")
    end
  end

  def add_bid_bulk_message(bulk_message,record)
    bid_data = record["bid"]

    bid_doc = Hash.new
    bid_doc["time"] = bid_data["time"]
    bid_doc["sapId"] = bid_data["sapId"]
    bid_doc["auctionId"] = bid_data["auctionId"]
    bid_doc["host"] = bid_data["host"]
    bid_doc["cur"] = bid_data["cur"]
    bid_doc["price"] = bid_data["price"]
    bid_doc["sspId"] = bid_data["sspId"]
    bid_doc["noBidReason"] = bid_data["noBidReason"]
    bid_doc["responseTime"] = bid_data["responseTime"]
    bid_doc["bidType"] = bid_data["bidType"]
    bid_doc["status"] = bid_data["status"]

    bid_doc["imp"] = Hash.new
    bid_doc["imp"]["w"] = bid_data["imp"]["w"]
    bid_doc["imp"]["h"] = bid_data["imp"]["h"]

    bid_doc["media"] = Hash.new
    bid_doc["media"]["type"] = bid_data["media"]["type"]
    bid_doc["media"]["id"] = bid_data["media"]["id"]
    bid_doc["media"]["domain"] = bid_data["media"]["domain"]

    bid_doc["device"] = Hash.new
    bid_doc["device"]["osv"] = bid_data["device"]["osv"]
    bid_doc["device"]["platform"] = bid_data["device"]["platform"]
    bid_doc["device"]["ids"] = bid_data["device"]["ids"]

    bid_doc["ad"] = Hash.new
    bid_doc["ad"]["id"] = bid_data["ad"]["id"]
    bid_doc["ad"]["cpnId"] = bid_data["ad"]["cpnId"]
    bid_doc["ad"]["creativeId"] = bid_data["ad"]["creativeId"]
    bid_doc["ad"]["appId"] = bid_data["ad"]["appId"]
    bid_doc["ad"]["advId"] = bid_data["ad"]["advId"]
    bid_doc["ad"]["sfe"] = bid_data["ad"]["sfe"]
    bid_doc["ad"]["adgId"] = bid_data["ad"]["adgId"]

    doc_id = create_doc_id(bid_doc["sapId"],bid_doc["auctionId"])
    target_index = "#{@logstash_prefix}-#{Time.at(time).strftime("#{@logstash_dateformat}")}"

    meta = { "create" => {"_index" => target_index, "_type" => @type_name, "_id" => doc_id} }
    action = { "script" => "ctx._source.bid = bid", "params" => {"bid" => bid_doc}}

    bulk_message << meta
    bulk_message << action
  end

  def create_doc_id(sap_id,auctionId)
    Digest::SHA256.hexdigest("#{sap_id}#{auctionId}")
  end

  def send(data)
    puts("data",data)
    client.bulk body: data
    puts("sended")
  end

end