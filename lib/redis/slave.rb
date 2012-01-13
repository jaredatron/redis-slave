require 'redis'
require 'childprocess'
require 'tempfile'
require 'tmpdir'
require "redis/slave/version"

class Redis::Slave

  attr_reader :process, :master_host, :master_port, :host, :port, :logfile, :dir

  def initialize options={}
    @master_host = options[:master_host] || '127.0.0.1'
    @master_port = options[:master_port] || '6379'
    @host        = options[:host]        || '127.0.0.1'
    @port        = options[:port]        || find_available_port
    @logfile     = options[:logfile]     || Tempfile.new('redis-slave-logfile').path
    @dir         = options[:dir]         || Dir.tmpdir
  end

  def redis_master
    ::Redis.new(:host => @master_host, :port => @master_port)
  end

  def redis_slave
    start!
    ::Redis.new(:host => @host, :port => @port)
  end

  def redis
    Balancer.new(redis_master, redis_slave)
  end

  def started?
    !!@started
  end

  def start!
    return if started?
    @process = ChildProcess.new('redis-server -')
    process.duplex = true
    process.start
    at_exit{ process.send(:send_kill) }
    process.io.stdin.puts config
    process.io.stdin.close
    @started = process.alive?
  end

  def config
<<-CONFIG
slaveof #{master_host} #{master_port}
slave-serve-stale-data yes
daemonize no
bind #{host}
port #{port}
logfile #{logfile}
databases 1
save 900 1
rdbcompression yes
dbfilename dump.rdb
dir #{dir}
appendonly no
appendfsync no
appendfsync everysec
no-appendfsync-on-rewrite yes
CONFIG
  end

  private

  # copied from  capybara-1.1.2
  def find_available_port
    server = TCPServer.new('127.0.0.1', 0)
    server.addr[1]
  ensure
    server.close if server
  end

end

require "redis/slave/balancer"
