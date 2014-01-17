require 'metriks/time_tracker'
require 'net/https'

module Metriks::Reporter
  class Datadog
    attr_accessor :prefix, :source, :url, :tags
    attr_reader :api_key

    def initialize(api_key, options = {})
      @api_key = api_key

      @prefix = options[:prefix]
      @source = options[:source]
      @url = "https://app.datadoghq.com/api/v1/series"
      @tags = options.fetch(:tags, [])

      @registry  = options[:registry] || Metriks::Registry.default
      @time_tracker = Metriks::TimeTracker.new(options[:interval] || 60)
      @on_error  = options[:on_error] || proc { |ex| }
    end

    def start
      @thread ||= Thread.new do
        loop do
          @time_tracker.sleep

          Thread.new do
            begin
              write
            rescue Exception => ex
              @on_error[ex] rescue nil
            end
          end
        end
      end
    end

    def stop
      @thread.kill if @thread
      @thread = nil
    end

    def restart
      stop
      start
    end

    def write
      gauges = []
      @registry.each do |name, metric|
        gauges << case metric
        when Metriks::Gauge
          prepare_metric(name, metric, :value)
        else
          prepare_metric(name, metric, :count)
        end
      end

      unless gauges.empty?
        submit(form_data(gauges))
      end
    end

    def submit(data)
      url = URI.parse(@url)
      req = Net::HTTP::Post.new(url.path + "?api_key=#{api_key}", initheader = {'Content-Type' =>'application/json'})
      req.body = JSON.dump(data)

      http = Net::HTTP.new(url.host, url.port)
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      http.use_ssl = true
      store = OpenSSL::X509::Store.new
      store.set_default_paths
      http.cert_store = store

      case res = http.start { |http| http.request(req) }
      when Net::HTTPSuccess, Net::HTTPRedirection
        # OK
      else
        res.error!
      end
    end

    def form_data(metrics)
      {series: metrics}
    end

    def prepare_metric(base_name, metric, key)
      time = @time_tracker.now_floored

      base_name = base_name.to_s.gsub(/ +/, '_')
      if @prefix
        base_name = "#{@prefix}.#{base_name}"
      end

      time = @time_tracker.now_floored
      value = metric.send(key)

      {
        :metric => base_name,
        :host => source,
        :points => [[time, value]],
        :type => 'gauge',
        :tags => tags,
      }
    end
  end
end
