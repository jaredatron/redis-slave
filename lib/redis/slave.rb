require 'redis'
require 'childprocess'
require 'tempfile'
require 'tmpdir'
require "redis/slave/version"

class Redis::Slave

  attr_reader :process, :options

  def initialize options={}
    @options = options;
    options[:master] ||= {}
    options[:slave]  ||= {}
    options[:master][:host] ||= '127.0.0.1'
    options[:master][:port] ||= '6379'
    options[:slave ][:host] ||= '127.0.0.1'
    options[:slave ][:port] ||= find_available_port
    options[:logfile]       ||= Tempfile.new('redis-slave-logfile').path
    options[:dir]           ||= Dir.tmpdir
  end

  def master
    ::Redis.new(options[:master])
  end

  def slave
    start!
    ::Redis.new(options[:slave])
  end

  def balancer
    Balancer.new(master, slave)
  end
  alias_method :redis, :balancer

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
slaveof #{options[:master][:host]} #{options[:master][:port]}
slave-serve-stale-data yes
daemonize no
bind #{options[:slave][:host]}
port #{options[:slave][:port]}
logfile #{options[:logfile]}
save 900 1
rdbcompression yes
dbfilename dump.rdb
dir #{options[:dir]}
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
