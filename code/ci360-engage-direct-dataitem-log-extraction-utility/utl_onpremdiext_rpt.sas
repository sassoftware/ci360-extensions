/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
%let c_outputDir=C:\Temp\onpremdiext;
libname outlib "&c_outputDir." compress=yes;

/*
Find all data items used in criteria nodes that were used in any task executed
after 2025-11-24T19:53:00.000
*/
Title List of all dataitems used in tasks executed after 2025-11-24T19:53:00.000 ;
proc sql number ;

	select n.node_id, n.dataitem_name, rt.externalCode, rt.task_runtime,rt.task_name, s.segmentMapName, n.node_name, n.node_type
	  from outlib.runtask rt
	  inner join outlib.node n
	  	on rt.runtask_id = n.runtask_id
	  left join outlib.segmap_data s
	  	on rt.runtask_id = s.runtask_id
	  	and find(s.clientData, trim(n.node_id), 'i') > 0
	where rt.task_runtime > '2025-11-24T19:53:00.000'dt
	order by rt.task_runtime
	  ;
quit;

/*
Which tasks used root.Marketingsegment in criteria nodes that were used in any task executed
after 2025-11-24T19:53:00.000
*/
Title Tasks that executed after 2025-11-24T19:53:00.000 and used root.Marketingsegment ;
proc sql number ;

	select n.node_id, n.dataitem_name, rt.externalCode, rt.task_runtime,rt.task_name, s.segmentMapName, n.node_name, n.node_type
	  from outlib.runtask rt
	  inner join outlib.node n
	  	on rt.runtask_id = n.runtask_id
	  left join outlib.segmap_data s
	  	on rt.runtask_id = s.runtask_id
		and find(s.clientData, trim(n.node_id), 'i') > 0
	where rt.task_runtime > '2025-11-24T19:53:00.000'dt
	  and n.dataitem_name = 'root.Marketingsegment'
	order by rt.task_runtime
	  ;
quit;

/*
Find all data items used in export nodes that were used in any task executed
after 2025-11-24T19:53:00.000
*/
Title Dataitems used in exports in Segment maps, Tasks that executed after 2025-11-24T19:53:00.000 ;

proc sql number ;

	select ex.externalCode, ex.dataitem_name, ex.outputName, rt.task_runtime,rt.task_name
	  from outlib.runtask rt
	  inner join outlib.export ex
	  	on rt.runtask_id = ex.runtask_id
	where rt.task_runtime > '2025-11-24T19:53:00.000'dt
	order by ex.outputName, ex.dataitem_name
	  ;
quit;
