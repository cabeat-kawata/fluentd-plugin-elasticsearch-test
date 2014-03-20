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

    time = record["time"]
    ssp_id = record["sspId"]
    auction_id = record["auctionId"]

    target_index = "#{@logstash_prefix}-#{Time.at(time).strftime("#{@logstash_dateformat}")}"

    doc_id = create_doc_id(ssp_id,auction_id)
    meta = { "create" => {"_index" => target_index, "_type" => @type_name, "_id" => doc_id} }
    action = { "script" => "ctx._source.bid = bid", "params" => {"bid" => record}}

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