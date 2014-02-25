# Ruby version of the Harvey harvester script
# ----------------------------------------------------------------------------
# TODO: support updates by including command line options for ListRecord from
# and until params.
#
# ----------------------------------------------------------------------------

require 'net/http'

class NDLTDDownloader
    def initialize(storage_dir)
        @storage_dir = storage_dir
        @resumption_token = nil
        @bytes_received = 0
        @files_written = 0
        @base_url = 'http://union.ndltd.org/OAI-PMH/'
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
            return URI("#{@base_url}?verb=ListRecords#{params}")
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

    # arg handling
    if ARGV.length == 0 or ARGV[0].length == 0
        raise ArgumentError, 'Expected output directory argument'
    else
        storage_dir = File.join(Dir.pwd, ARGV[0])
        if not Dir.exists?(storage_dir)
            Dir.mkdir(ARGV[0])
        end
    end

    NDLTDDownloader.new(storage_dir).run()
end
