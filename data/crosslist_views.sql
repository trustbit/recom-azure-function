create view crosslist_test.cross_series as
select c.id,
       product_series_id as cross_left_id,
       product_series_cross_id as cross_right_id,
       level as cross_level,
       notes,
       ps.name as cross_left_name,
       ps_c.name as cross_right_name
from crosslist_test.crosses as c
inner join crosslist_test.product_series as ps on ps.id = c.product_series_id
inner join crosslist_test.product_series as ps_c on ps_c.id = c.product_series_cross_id;


create view crosslist_test.cross_part_number as

with cross_series as (select c.id,
       product_series_id as cross_left_id,
       product_series_cross_id as cross_right_id,
       level as cross_level,
       notes,
       ps.name as cross_left_name,
       ps_c.name as cross_right_name
from crosslist_test.crosses as c
inner join crosslist_test.product_series as ps on ps.id = c.product_series_id
inner join crosslist_test.product_series as ps_c on ps_c.id = c.product_series_cross_id)

select cross_series.id,
       cross_left_id,
       cross_right_id,
       cross_level,
       notes,
       cross_left_name,
       cross_right_name,
       c_left.id as left_id,
       c_left.company as left_company,
       c_left.part_number as left_part_number ,
       c_left.converter_type as left_converter_type ,
       string_agg(certs_left.) as protections,
       c_left.ac_voltage_input_min as left_ac_voltage_input_min ,
       c_left.ac_voltage_input_max as left_ac_voltage_input_max ,
       c_left.dc_voltage_input_min as left_dc_voltage_input_min ,
       c_left.dc_voltage_input_max as left_dc_voltage_input_max ,
       c_left.input_voltage_tolerance as left_input_voltage_tolerance ,
       c_left.power as left_power ,
       c_left.is_regulated as left_is_regulated ,
       c_left.regulation_voltage_range as left_regulation_voltage_range ,
       c_left.efficiency as left_efficiency ,
       c_left.voltage_output_1 as left_voltage_output_1 ,
       c_left.voltage_output_2 as left_voltage_output_2 ,
       c_left.voltage_output_3 as left_voltage_output_3 ,
       c_left.i_out1 as left_i_out1 ,
       c_left.i_out2 as left_i_out2 ,
       c_left.i_out3 as left_i_out3 ,
       c_left.output_type as left_output_type ,
       c_left.pin_count as left_pin_count ,
       c_left.mounting_type as left_mounting_type ,
       c_left.connection_type as left_connection_type ,
       c_left.dimensions_unit as left_dimensions_unit ,
       c_left.dimensions_length as left_dimensions_length ,
       c_left.dimensions_width as left_dimensions_width ,
       c_left.dimensions_height as left_dimensions_height ,
       c_left.operating_temp_min as left_operating_temp_min ,
       c_left.operating_temp_max as left_operating_temp_max ,
       c_left.created_at as left_created_at ,
       c_left.updated_at as left_updated_at ,
       c_right.id as  right_id ,
       c_right.company as  right_company ,
       c_right.part_number as  right_part_number ,
       c_right.converter_type as  right_converter_type ,
       c_right.ac_voltage_input_min as  right_ac_voltage_input_min ,
       c_right.ac_voltage_input_max as  right_ac_voltage_input_max ,
       c_right.dc_voltage_input_min as  right_dc_voltage_input_min ,
       c_right.dc_voltage_input_max as  right_dc_voltage_input_max ,
       c_right.input_voltage_tolerance as  right_input_voltage_tolerance ,
       c_right.power as  right_power ,
       c_right.is_regulated as  right_is_regulated ,
       c_right.regulation_voltage_range as  right_regulation_voltage_range ,
       c_right.efficiency as  right_efficiency ,
       c_right.voltage_output_1 as  right_voltage_output_1 ,
       c_right.voltage_output_2 as  right_voltage_output_2 ,
       c_right.voltage_output_3 as  right_voltage_output_3 ,
       c_right.i_out1 as  right_i_out1 ,
       c_right.i_out2 as  right_i_out2 ,
       c_right.i_out3 as  right_i_out3 ,
       c_right.output_type as  right_output_type ,
       c_right.pin_count as  right_pin_count ,
       c_right.mounting_type as  right_mounting_type ,
       c_right.connection_type as  right_connection_type ,
       c_right.dimensions_unit as  right_dimensions_unit ,
       c_right.dimensions_length as  right_dimensions_length ,
       c_right.dimensions_width as  right_dimensions_width ,
       c_right.dimensions_height as  right_dimensions_height ,
       c_right.operating_temp_min as  right_operating_temp_min ,
       c_right.operating_temp_max as  right_operating_temp_max ,
       c_right.created_at as  right_created_at,
       c_right.updated_at as right_updated_at
from cross_series

inner join crosslist_test.converters as c_left on cross_series.cross_left_id = c_left.product_series_id
inner join crosslist_test.converters as c_right on cross_series.cross_right_id = c_right.product_series_id;

