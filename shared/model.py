import json
# IMPORTANT!
# DO NOT TOUCH THIS FILE UNLESS YOU UNDERSTAND WHAT YOU ARE DOING
# AND HAVE A TESTING SETUP TO TEST ANY CHANGE
# ORDER OF FIELDS AND NAMING ARE IMPORTANT TO CUSTOM CHAIN OF THOUGHT
# DOING AD HOC CHANGES COULD DECREASE THE QUALITY DRAMATICALLY
# CHANGES WILL ALSO REQUIRE RERUNNING THE PIPELINE

from typing import Optional, Dict, List, Literal, Union

from pydantic import BaseModel, Field

class Dimensions(BaseModel):
    unit: Literal["mm", "in"]
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None

class TemperatureRange(BaseModel):

    min: Optional[float] = None
    max: Optional[float] = None

class Size(BaseModel):
    unit: Literal["mm", "in"]
    length: Optional[float]
    width: Optional[float]
    height: Optional[float]


class Temperature(BaseModel):
    temperature: int
    unit: Literal["C", "F"]


# DO NOT CHANGE ANYTHING. READ FILE HEADER FIRST
class PowerDerating(BaseModel):
    threshold: Optional[Temperature] = None
    unit: Literal["W", "%", "%/K", "%/C"] = Field(..., description="Unit of output power derating - percent, Watt or rate (% per C/K)")
    rate: float = Field(..., description="Value of power derating at the temperature")



# DO NOT CHANGE ANYTHING. READ FILE HEADER FIRST
MountingTypes = Literal["PCB Mount", "DIN-Rail", "Chassis Mount", "Panel Mount", "Wired", "Other"]
ConnectionTypes = Literal["THT", "SMD", "BGA", "Wire", "Terminal", "Other"]
ComponentStyle = Literal["DIP", "QFP", "SOIC", "QFN", "LGA", "SOP","SIP", "LOC","Other"]

OutputTypes = Literal["Single", "Dual", "Triple", "Other"]

PackagingType = Literal["Tube", "Tape & Reel", "Tray", "Cardboard Box", "Single Box", "Airbag", "Moisture Barrier Bag", "Other"]
ConverterTypes = Literal["DC/DC", "AC/DC"]


# DO NOT CHANGE ANYTHING. READ FILE HEADER FIRST
class ComponentPackage(BaseModel):
    package_name: Optional[str] = Field(..., description="Component package name, e.g. SIP-7, LGA-24, Half-Brick etc")
    mounting_type: Optional[MountingTypes] = Field(None, description="How is the component installed")
    connection_type: Optional[ConnectionTypes] = Field(None, description="How is component connected electrically")
    style: Optional[ComponentStyle] = None
    # removed, since that confuses LLM - in case of SIP-7 with 5 actual pins,
    # is it 7 or 5?
    #number_of_pins_in_footprint: Optional[int] = None
    brick_size: Optional[Literal["Half", "Quarter", "Eighth", "Sixteenth", "Thirty-second", "other"]] = None
    ip_rating: Optional[Literal["IP20", "IP67", "IP68", "IP69K", "other"]] = None


# DO NOT CHANGE ANYTHING. READ FILE HEADER FIRST
PinType = Literal[
    "Gnd", "-VDC in", "-VDC in (GND)",
    "+VDC in",
    "VAC in (L)", "VAC in (N)", "+V out", "-V out", "Signal", "Remote On/Off",
    "No Connection (NC)",
    "+Sense", "-Sense",
    "Trim", "CTRL/UVLO (Under Voltage Lock Out)", "Other",
    "Case", # to ground the case, proding EMP shielding or heat dissipation
    #"No pin", # removed this. Since no pin should be just skipped
    "Common", # reference pin in dual output converters,
    "Frequency Sync"
]


class Pin(BaseModel):
    pin_id: Optional[Union[int, str]] = Field(None, description="Pin number or name")
    type: PinType


class IsolationTestVoltage(BaseModel):
    duration_sec: Optional[int] = None
    unit: Literal["VDC", "VAC", "Unknown"]
    voltage: int

# DO NOT CHANGE ANYTHING. READ FILE HEADER FIRST
class PowerConverterModel(BaseModel):
    product_series: Optional[str] = Field(None, description="Product series, examples: RxxPxx/R, RAC02E-K/277, RxxC05TExxS, RFMM, RPMD15-FW")
    part_number: str = Field(..., description="Part/Model number. Pay attention to model numbering rules. Examples: RAC04-12SK/277, RACM65-24S-ST, REM2A-0515S, RP30-2415SFW/N-HC")
    converter_type: ConverterTypes = Field(..., description="Converter type (DC/DC, AC/DC)")

    ac_voltage_input_min: Optional[float] = Field(None, description="Minimum input AC voltage")
    ac_voltage_input_max: Optional[float] = Field(None, description="Maximum input AC voltage")
    dc_voltage_input_min: Optional[float] = Field(None, description="Minimum input DC voltage")
    dc_voltage_input_max: Optional[float] = Field(None, description="Maximum input DC voltage")

    input_voltage_tolerance: Optional[float] = Field(None, description="Typical input voltage tolerance in percent. E.g. typ. ±5%")

    power: Optional[float] = Field(None, description="Power in watts")

    is_regulated: Optional[bool] = Field(None, description="Is this converter regulated?")
    regulation_voltage_range: Optional[str] = Field(None, description="Input voltage range. E.g. 10:1")
    efficiency: Optional[float] = Field(None, description="Efficiency in percentage")

    isolation_test_voltage: List[IsolationTestVoltage] = Field(...)

    voltage_output_1: Optional[float] = Field(None, description="Output voltage 1")
    voltage_output_2: Optional[float] = Field(None, description="Output voltage 2")
    voltage_output_3: Optional[float] = Field(None, description="Output voltage 3")
    i_out1: Optional[float] = Field(None, description="Output current 1 in amperes")
    i_out2: Optional[float] = Field(None, description="Output current 2 in amperes")
    i_out3: Optional[float] = Field(None, description="Output current 3 in amperes")

    # output type is obvious at this point, but can help LLM to pick the right pins
    # from the pin table
    output_type: Optional[OutputTypes] = Field(None, description="Output type (Single, Dual, Triple, etc.)")
    # pins need output type
    pins: List[Pin] = Field(..., description="Pin configuration. Pay attention to component output type")
    package: Optional[ComponentPackage] = Field(None, description="Component package information")

    packaging_type: Optional[PackagingType] = Field(None, description="Packaging type (e.g., Tube, Tape & Reel, Tray, Cardboard Box, Single Box, Airbag, Moisture Barrier Bag, Other)")

    dimensions: Optional[Dimensions] = Field(None, description="Dimensions in mm: length, width, height")
    certifications: Optional[List[str]] = Field(None, description="List of certifications")
    protections: Optional[List[str]] = Field(None, description="List of protection features")

    # table or chart-based usually
    operating_temperature: Optional[TemperatureRange] = Field(None, description="Operating temperature range")
    # checklist style, to lead LLM to focus and fill
    #has_high_temperature_power_derating: Optional[bool] = None
    power_derating: List[PowerDerating] = Field(..., description="How does temperature affect output power?")






PowerConverterModel_schema = json.dumps(PowerConverterModel.model_json_schema(), indent=2, ensure_ascii=False)


class PowerConverterList(BaseModel):
    # repeat, so that it is clear that this is a list of models
    part_numbers_to_extract: List[str]
    power_converters: List[PowerConverterModel]



class PdfPageExtract(BaseModel):
    raw_text: str
    latex: str


class PdfExtract(BaseModel):
    pages: List[PdfPageExtract]
