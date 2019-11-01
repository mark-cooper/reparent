require 'digest'
require 'logger'
require 'sequel'
require 'yaml'

config = YAML.load_file(
  File.join(__dir__, 'config.yml')
).transform_keys(&:to_sym)

DB = Sequel.connect({
  adapter: :mysql2,
  max_connections: 20,
  logger: Logger.new(STDOUT)
}.merge(config[:db]))

def setup_new_parent_for(child, title, level, ref_id)
  parent = {}
  parent[:lock_version] = 1
  parent[:json_schema_version] = 1
  parent[:repo_id] = child[:repo_id]
  parent[:root_record_id] = child[:root_record_id]
  parent[:ref_id] = ref_id
  parent[:parent_id] = child[:parent_id]
  parent[:parent_name] = child[:parent_name]
  parent[:position] = child[:position] + 1
  parent[:publish] = 1
  parent[:title] = title
  parent[:display_string] = title
  parent[:level_id] = level
  parent[:created_by] = 'admin'
  parent[:last_modified_by] = 'admin'
  parent[:create_time] = Time.now
  parent[:system_mtime] = Time.now
  parent[:user_mtime] = Time.now
  parent
end

reparent = config[:reparent].transform_keys(&:to_sym)
parents  = {}

DB[:archival_object].where(
  root_record_id: reparent[:root_record_id],
  parent_name: reparent[:parent_name]
).each do |child|
  title = child[:title][0..2]
  ref_id = Digest::SHA1.hexdigest(title)

  unless parents.key? title
    if DB[:archival_object].where(ref_id: ref_id).count.zero?
      parent = setup_new_parent_for(child, title, reparent[:level_id], ref_id)
      DB[:archival_object].insert(parent)
      puts "Created parent: #{title}, #{ref_id}"
    end
    parent = DB[:archival_object].where(ref_id: ref_id).first
    parents[title] = parent
  end

  child.update(
    parent_id: parents[title][:id],
    parent_name: "#{parents[title][:id]}@archival_object",
    system_mtime: Time.now
  )
  puts "Updated: #{child[:id]}, #{child[:title]} -- #{parents[title][:id]}, #{title}"
end

puts
puts "Parents processed: #{parents.count}"
