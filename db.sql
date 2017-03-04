create table files(
	`id` int primary key auto_increment,
	`path` text,
	`status` set('NEW', 'SEEN', 'PROBING', 'PROBED', 'SPLITING', 'SPLITED', 'ENCODING', 'ENCODED', 'MERGING-UGLY', 'MERGED-UGLY', 'THUMBING', 'THUMBED', 'MERGING', 'MERGED', 'FAILED'),
	metadata text default null, 
	master int default null,
	chunkid int default null,
	hostname text default null,
	thumbs longtext default null,
	video int default null
);
