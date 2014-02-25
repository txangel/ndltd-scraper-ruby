# Ruby version of the Harvey harvester script
# ----------------------------------------------------------------------------
# TODO: support updates by including command line options for ListRecord from
# and until params.
#
# ----------------------------------------------------------------------------

require 'net/http'

class NDLTDScraper
    def initialize(storage_dir)
        @storage_dir = storage_dir
    end

    def run
        base_url = 'http://union.ndltd.org/OAI-PMH/'

        file_id = 0

        resumption_token = nil

        bytes_received = 0

        begin
            uri = URI("#{base_url}?verb=ListRecords&" + ( (resumption_token) ? "resumptionToken=#{resumption_token}" : 'metadataPrefix=oai_dc' ) )

            puts "Harvesting #{uri}"

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

            content = res.body

            puts "Got response of #{content.length} bytes"

            bytes_received += content.length

            file = File.join(@storage_dir, "data_#{file_id}.xml")

            puts "Writing to #{file}"
            File.open(file, 'w') {|f| f.write(content)}

            file_id += 1

            resumption_token = nil
            if content =~ /<resumptionToken[^>]*>([^<]+)<\/resumptionToken>/
                puts "Detected resumption token"
                resumption_token = $1
            end

        end while resumption_token

        puts "Final bytes: #{bytes_received}"
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

    d = NDLTDScraper.new(storage_dir)
    d.run()

end
