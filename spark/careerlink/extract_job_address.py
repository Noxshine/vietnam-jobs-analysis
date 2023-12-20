import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def extract_job_address(input: str):
    if 'Hà Nội' in input:
        return 'HaNoi'
    if 'Hồ Chí Minh' in input:
        return 'HoChiMinh'
    if 'Đồng Nai' in input:
        return 'DongNai'
    if 'Bình Dương' in input:
        return 'BinhDuong'
    if 'Hải Dương' in input:
        return 'HaiDuong'
    if 'Hải Phòng' in input:
        return 'HaiPhong'
    if 'Đà Nẵng' in input:
        return 'DaNang'
    if 'Cần Thơ' in input:
        return 'CanTho'
    if 'Hà Nam' in input:
        return 'HaNam'
    if 'Nghệ An' in input:
        return 'NgheAn'
    if 'Hưng Yên' in input:
        return 'HungYen'
    if 'Thái Bình' in input:
        return 'ThaiBinh'
    if 'Thanh Hóa' in input:
        return 'ThanhHoa'
    if 'Long An' in input:
        return 'LongAn'
    if 'Quảng Nam' in input:
        return 'QuangNam'
    if 'Quảng Ninh' in input:
        return 'QuangNinh'
    if 'Quảng Ngãi' in input:
        return 'QuangNgai'
    if 'Vĩnh Long' in input:
        return 'VinhLong'
    if 'Kiên Giang' in input:
        return 'KienGiang'
    if 'Thừa Thiên- Huế' in input:
        return 'Hue'
    if 'An Giang' in input:
        return 'AnGiang'
    if 'Đồng Tháp' in input:
        return 'DongThap'
    if 'Bình Định' in input:
        return 'BinhDinh'
    if 'Bắc Ninh' in input:
        return 'BacNinh'
    if 'Bắc Giang' in input:
        return 'BacGiang'
    if 'Bắc Cạn' in input:
        return 'BacCan'
    if 'Lâm Đồng' in input:
        return 'LamDong'
    if 'Tiền Giang' in input:
        return 'TienGiang'
    if 'Khánh Hòa' in input:
        return 'KhanhHoa'
    if 'Lào Cai' in input:
        return 'LaoCai'
    if 'Vĩnh Phúc' in input:
        return 'VinhPhuc'
    if 'Bạc Liêu' in input:
        return 'BacLieu'
    if 'Dak Lak' in input:
        return 'DakLak'
    if 'Phú Yên' in input:
        return 'PhuYen'
    if 'Nam Định' in input:
        return 'NamDinh'
    if 'Ninh Bình' in input:
        return 'NinhBinh'
    if 'Hà Giang' in input:
        return 'HaGiang'
    if 'An Giang' in input:
        return 'AnGiang'
    if 'Vũng Tàu' in input:
        return 'VungTau'
    if 'Bắc Kạn' in input:
        return 'BacKan'
    if 'Bến Tre' in input:
        return 'BenTre'
    if 'Bình Phước' in input:
        return 'BinhPhuoc'
    if 'Bình Thuận' in input:
        return 'BinhThuan'
    if 'Cà Mau' in input:
        return 'CaMau'
    if 'Cao Bằng' in input:
        return 'CaoBang'
    if 'Đắk Nông' in input:
        return 'DakNong'
    if 'Điện Biên' in input:
        return 'DienBien'
    if 'Gia Lai' in input:
        return 'GiaLai'
    if 'Hà Tĩnh' in input:
        return 'HaTinh'
    if 'Hậu Giang' in input:
        return 'HauGiang'
    if 'Hòa Bình' in input:
        return 'HoaBinh'
    if 'Kon Tum' in input:
        return 'KonTum'
    if 'Lai Châu' in input:
        return 'LaiChau'
    if 'Lạng Sơn' in input:
        return 'LangSon'
    if 'Ninh Thuận' in input:
        return 'NinhThuan'
    if 'Phú Thọ' in input:
        return 'PhuTho'
    if 'Quảng Bình' in input:
        return 'QuangBinh'
    if 'Quảng Trị' in input:
        return 'QuangTri'
    if 'Sóc Trăng' in input:
        return 'SocTrang'
    if 'Sơn La' in input:
        return 'SonLa'
    if 'Tây Ninh' in input:
        return 'TayNinh'
    if 'Thái Nguyên' in input:
        return 'ThaiNguyen'
    if 'Trà Vinh' in input:
        return 'TraVinh'
    if 'Tuyên Quang' in input:
        return 'TuyenQuang'
    if 'Vĩnh Long' in input:
        return 'VinhLong'
    if 'Yên Bái' in input:
        return 'YenBai'

    return "not-found"
