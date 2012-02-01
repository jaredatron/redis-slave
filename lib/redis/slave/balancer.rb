class Redis::Slave::Balancer

  undef_method :type if instance_methods.include?('type')

  MUTATING_METHODS = %w{
    []=
    set hmset mset lset mapped_hmset getset msetnx setex hset setnx hsetnx
    del hdel
    srem zremrangebyscore zremrangebyrank zrem lrem
    spop brpop rpop blpop lpop
    lpush rpush
    rpoplpush
    incr zincrby incrby hincrby
    decrby decr
    ltrim
    move
    renamenx rename
    save
    subscribe unsubscribe
  }

  attr_accessor :master, :slave

  def initialize master, slave
    @master, @slave = master, slave
  end

  def method_missing method, *args, &block
    redis = MUTATING_METHODS.include?(method.to_s) ? @master : @slave
    redis.send(method, *args, &block)
  end

  def inspect
    %[#<#{self.class} master=#{@master.inspect} slave=#{@slave.inspect}>]
  end

end
