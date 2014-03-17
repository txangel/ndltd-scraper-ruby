# Ruby version of the Harvey harvester script
# ----------------------------------------------------------------------------
# TODO: support updates by including command line options for ListRecord from
# and until params.
#
# ----------------------------------------------------------------------------

require 'net/http'
require 'optparse'

class NDLTDDownloader
    def initialize(storage_dir, token = nil, start_date = nil, end_date = nil)
        @storage_dir = storage_dir
        @start_date = Time.parse(start_date).iso8601
        @end_date = end_date
        @resumption_token = token
        @bytes_received = 0
        @files_written = 0
        @base_url = 'http://union.ndltd.org/OAI-PMH/'


        if @resumption_token
            parts = token.split('!')
            @files_written = parts[4].to_i / 1000
        end

        if @start_date
            @start_date = Time.parse(start_date).iso8601
        end

        if @end_date
            @end_date = Time.parse(end_date).iso8601
        end

    end

    def run
        begin
            # get xml content
            content = get_xml_response(build_uri()).body

            # increase bytes received
            @bytes_received += content.length

            # write file to storage
            store_xml_response(content)

            # pull next resumption token
            update_resumption_token(content)

            # print percentage
            print_percentage_done(@resumption_token)
        end while @resumption_token
    end

    def reset
        @resumption_token = nil
        @bytes_received = 0
        @files_written = 0
    end

    private

        def build_uri()
            params = (@resumption_token ? "&resumptionToken=#{@resumption_token}" : '&metadataPrefix=oai_dc')
            return URI(URI.encode("#{@base_url}?verb=ListRecords#{params}"))
        end

        def get_xml_response(uri)
            res = Net::HTTP.get_response(uri)
            while res.code == 503
                sleep_time = res['Retry-After'].to_i
                if sleep_time >= 0 and sleep_time <= 86400
                    puts "Sleeping for #{sleep_time}s"
                    sleep(sleep_time)
                    res = Net::HTTP.get_response(uri)
                else
                    raise ArgumentError, "invalid sleep time #{res['Retry-After']}"
                end
            end
            return res
        end

        def store_xml_response(xml)
            file = File.join(@storage_dir, "data_#{@files_written}.xml")
            puts "Writing to #{file}"
            File.open(file, 'w') {|f| f.write(xml)}
            @files_written += 1
        end

        def update_resumption_token(str)
            if str =~ /<resumptionToken[^>]*>([^<]+)<\/resumptionToken>/
                @resumption_token = $1
            else
                @resumption_token = nil
            end
        end

        def print_percentage_done(token)
            if token
                parts = token.split('!')
                percent = parts[4].to_f / parts[5].to_f * 100
                puts "#{percent.round(2)}%"
            else
                puts "100%"
            end
        end
end

if __FILE__ == $PROGRAM_NAME
    storage_dir = nil

    token = nil
    start_date = nil
    end_date = nil

    OptionParser.new do |opts|
      opts.banner = "Usage: ndltd_downloader.rb <output directory> [--token resumption_token]"

      opts.on('-t', '--token ARG', 'Start from a resumptionToken') do |arg|
        token = arg
      end

      opts.on('-s', '--start_date ARG', 'All documents changed from this date (iso8601)') do |arg|
        start_date = arg
      end

      opts.on('-e', '--end_date ARG', 'All documents changed until this date (iso8601)') do |arg|
        end_date = arg
      end
    end.parse!

    # arg handling
    if ARGV.length == 0 or ARGV[0].length == 0
        raise ArgumentError, 'Expected output directory argument'
    else
        storage_dir = File.join(Dir.pwd, ARGV[0])
        if not Dir.exists?(storage_dir)
            Dir.mkdir(ARGV[0])
        end

        NDLTDDownloader.new(storage_dir, token start_date, end_date).run()
    end
end
