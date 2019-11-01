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

def setup_new_parent_for(child, title, level, ref_id, position)
  parent = {}
  parent[:lock_version] = 1
  parent[:json_schema_version] = 1
  parent[:repo_id] = child[:repo_id]
  parent[:root_record_id] = child[:root_record_id]
  parent[:ref_id] = ref_id
  parent[:parent_id] = child[:parent_id]
  parent[:parent_name] = child[:parent_name]
  parent[:position] = position
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
# 3 letter prefix => parent db record
parents  = {}
# non conflicting starting position for parents
starting_position = 50_000_000
parents_position  = starting_position
# 3 letter prefix => position for child object
parents_child_position = Hash.new(0)

DB[:archival_object].where(
  root_record_id: reparent[:root_record_id],
  parent_name: reparent[:parent_name]
).each do |child|
  child.update(title: "#{child[:title]}???") unless child[:title].length >= 3

  title = child[:title][0..2]
  ref_id = Digest::SHA1.hexdigest(title)

  unless parents.key? title
    if DB[:archival_object].where(ref_id: ref_id).count.zero?
      parent = setup_new_parent_for(
        child,
        title,
        reparent[:level_id],
        ref_id,
        parents_position
      )
      DB[:archival_object].insert(parent)
      puts "Created parent: #{title}, #{ref_id}"
    end
    parent = DB[:archival_object].where(ref_id: ref_id).first
    parents[title] = parent
    parents_position += 500
  end

  next if child[:ref_id] == ref_id

  child.update(
    parent_id: parents[title][:id],
    parent_name: "#{parents[title][:id]}@archival_object",
    position: parents_child_position[title],
    system_mtime: Time.now
  )
  puts "Updated: [#{parents[title][:id]}, #{title}, #{parents[title][:position]}] [#{child[:id]}, #{child[:title]}, #{child[:position]}]"
  parents_child_position[title] += 500
end

DB[:archival_object].where(
  root_record_id: reparent[:root_record_id],
  parent_name: reparent[:parent_name]
).each do |child|
  title = child[:title][0..2]
  ref_id = Digest::SHA1.hexdigest(title)
  next unless child[:ref_id] == ref_id

  position = parents[title][:position] - starting_position
  next unless position > 0

  parents[title].update(
    position: position,
    system_mtime: Time.now
  )
end

puts
puts "Parents processed: #{parents.count}"
