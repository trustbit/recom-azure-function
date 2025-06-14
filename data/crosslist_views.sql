create view crosslist.cross_series as

select c.id,
       product_series_id as cross_left_id,
       product_series_cross_id as cross_right_id,
       level as cross_level,
       notes,
       ps.name as cross_left_name,
       ps_c.name as cross_right_name
from crosslist.crosses as c
inner join crosslist.product_series as ps on ps.id = c.product_series_id
inner join crosslist.product_series as ps_c on ps_c.id = c.product_series_cross_id;



create view crosslist.cross_part_number as

   with cross_series as (select c.id,
       product_series_id as cross_left_id,
       product_series_cross_id as cross_right_id,
       level as cross_level,
       notes,
       ps.name as cross_left_name,
       ps_c.name as cross_right_name
from crosslist.crosses as c
inner join crosslist.product_series as ps on ps.id = c.product_series_id
inner join crosslist.product_series as ps_c on ps_c.id = c.product_series_cross_id),

        converters_certificate_mapping as (
    select
        converters.id,
        string_agg(name, ',') as certifications
    from crosslist.converters
        inner join crosslist.converter_certifications as cc on converters.id = cc.converter_id
        inner join crosslist.certifications as c on cc.certification_id = c.id
    group by converters.id),

    converters_protections_mapping as (
        select
            converters.id,
        string_agg(p.name, ',') as protections
        from crosslist.converters
        inner join crosslist.converter_protections as cp on converters.id = cp.converter_id
        inner join crosslist.protections as p on cp.protection_id = p.id
    group by converters.id),

    converters_isolation_test_mapping as (
    select
        c.id,
        string_agg(concat(it.voltage, '-', it.unit, ': ', it.duration_sec, 's'), ', ') as isolation_tests
    from crosslist.converters as c
             inner join crosslist.isolation_tests  as it on c.id = it.converter_id

    group by c.id),

product_cross as (
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
       c_right.updated_at as right_updated_at,
       cmp_left.certifications as certifications_cross_left,
       cpm_left.protections as protections_cross_left,
       cmp_left.certifications as certifications_cross_right,
       cpm_right.protections as protections_cross_right,
       cim_left.isolation_tests as isolation_test_duration_left,
       cim_right.isolation_tests as isolation_tests_duration_right


from cross_series

inner join crosslist.converters as c_left on cross_series.cross_left_id = c_left.product_series_id and c_left.company = 'recom'
inner join crosslist.converters as c_right on cross_series.cross_right_id = c_right.product_series_id and c_right.company <> 'recom'

    left outer join converters_certificate_mapping as cmp_left on cross_left_id = cmp_left.id
    left outer join converters_certificate_mapping as cmp_right on cross_right_id = cmp_left.id

left outer join converters_protections_mapping as cpm_left on cross_left_id = cpm_left.id
left outer join converters_protections_mapping as cpm_right on cross_right_id = cpm_right.id

left outer join converters_isolation_test_mapping as cim_left on cross_left_id = cim_left.id
left outer join converters_isolation_test_mapping as cim_right on cross_right_id = cim_right.id

)

select * from product_cross
;

create view crosslist.find_crosses as

with
    converters_certificate_mapping as (
    select
        converters.id,
        string_agg(name, ',') as certifications
    from crosslist.converters
        inner join crosslist.converter_certifications as cc on converters.id = cc.converter_id
        inner join crosslist.certifications as c on cc.certification_id = c.id
    group by converters.id),

    converters_protections_mapping as (
        select
            converters.id,
        string_agg(p.name, ',') as protections
        from crosslist.converters
        inner join crosslist.converter_protections as cp on converters.id = cp.converter_id
        inner join crosslist.protections as p on cp.protection_id = p.id
    group by converters.id),

    converters_isolation_test_mapping as (
        select
            c.id,
            string_agg(concat(it.voltage, '-', it.unit, ': ', it.duration_sec, 's'), ', ') as isolation_tests
        from crosslist.converters as c
                 inner join crosslist.isolation_tests  as it on c.id = it.converter_id
        group by c.id
        )

select
--     product_series.id,
       name,
       c.id,
       company,
       product_series_id,
       part_number,
       converter_type,
       ac_voltage_input_min,
       ac_voltage_input_max,
       dc_voltage_input_min,
       dc_voltage_input_max,
       input_voltage_tolerance,
       power,
       is_regulated,
       regulation_voltage_range,
       efficiency,
       voltage_output_1,
       voltage_output_2,
       voltage_output_3,
       i_out1,
       i_out2,
       i_out3,
       output_type,
       pin_count,
       mounting_type,
       connection_type,
       dimensions_unit,
       dimensions_length,
       dimensions_width,
       dimensions_height,
       operating_temp_min,
       operating_temp_max,
       created_at,
       updated_at,
--        cpm.id,
       protections,
--        ccm.id,
       certifications,
       isolation_tests
from crosslist.product_series
    left outer join crosslist.converters c on product_series.id = c.product_series_id
inner join converters_protections_mapping as cpm on c.id = cpm.id
         left outer join converters_certificate_mapping as ccm on ccm.id = c.id
left outer join converters_isolation_test_mapping as cim on cim.id = c.id

--where product_series.name = 'REM1'
;
