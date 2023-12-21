import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def modify_job_address(x: str):
    if 'Hà Nội' in x:
        return 'HaNoi'
    if 'Hồ Chí Minh' in x:
        return 'HoChiMinh'
    if 'Đồng Nai' in x:
        return 'DongNai'
    if 'Bình Dương' in x:
        return 'BinhDuong'
    if 'Hải Dương' in x:
        return 'HaiDuong'
    if 'Hải Phòng' in x:
        return 'HaiPhong'
    if 'Đà Nẵng' in x:
        return 'DaNang'
    if 'Cần Thơ' in x:
        return 'CanTho'
    if 'Hà Nam' in x:
        return 'HaNam'
    if 'Nghệ An' in x:
        return 'NgheAn'
    if 'Hưng Yên' in x:
        return 'HungYen'
    if 'Thái Bình' in x:
        return 'ThaiBinh'
    if 'Thanh Hóa' in x:
        return 'ThanhHoa'
    if 'Long An' in x:
        return 'LongAn'
    if 'Quảng Nam' in x:
        return 'QuangNam'
    if 'Quảng Ninh' in x:
        return 'QuangNinh'
    if 'Quảng Ngãi' in x:
        return 'QuangNgai'
    if 'Vĩnh Long' in x:
        return 'VinhLong'
    if 'Kiên Giang' in x:
        return 'KienGiang'
    if 'Thừa Thiên- Huế' in x:
        return 'Hue'
    if 'An Giang' in x:
        return 'AnGiang'
    if 'Đồng Tháp' in x:
        return 'DongThap'
    if 'Bình Định' in x:
        return 'BinhDinh'
    if 'Bắc Ninh' in x:
        return 'BacNinh'
    if 'Bắc Giang' in x:
        return 'BacGiang'
    if 'Bắc Cạn' in x:
        return 'BacCan'
    if 'Lâm Đồng' in x:
        return 'LamDong'
    if 'Tiền Giang' in x:
        return 'TienGiang'
    if 'Khánh Hòa' in x:
        return 'KhanhHoa'
    if 'Lào Cai' in x:
        return 'LaoCai'
    if 'Vĩnh Phúc' in x:
        return 'VinhPhuc'
    if 'Bạc Liêu' in x:
        return 'BacLieu'
    if 'Dak Lak' in x:
        return 'DakLak'
    if 'Phú Yên' in x:
        return 'PhuYen'
    if 'Nam Định' in x:
        return 'NamDinh'
    if 'Ninh Bình' in x:
        return 'NinhBinh'
    if 'Hà Giang' in x:
        return 'HaGiang'
    if 'An Giang' in x:
        return 'AnGiang'
    if 'Vũng Tàu' in x:
        return 'VungTau'
    if 'Bắc Kạn' in x:
        return 'BacKan'
    if 'Bến Tre' in x:
        return 'BenTre'
    if 'Bình Phước' in x:
        return 'BinhPhuoc'
    if 'Bình Thuận' in x:
        return 'BinhThuan'
    if 'Cà Mau' in x:
        return 'CaMau'
    if 'Cao Bằng' in x:
        return 'CaoBang'
    if 'Đắk Nông' in x:
        return 'DakNong'
    if 'Điện Biên' in x:
        return 'DienBien'
    if 'Gia Lai' in x:
        return 'GiaLai'
    if 'Hà Tĩnh' in x:
        return 'HaTinh'
    if 'Hậu Giang' in x:
        return 'HauGiang'
    if 'Hòa Bình' in x:
        return 'HoaBinh'
    if 'Kon Tum' in x:
        return 'KonTum'
    if 'Lai Châu' in x:
        return 'LaiChau'
    if 'Lạng Sơn' in x:
        return 'LangSon'
    if 'Ninh Thuận' in x:
        return 'NinhThuan'
    if 'Phú Thọ' in x:
        return 'PhuTho'
    if 'Quảng Bình' in x:
        return 'QuangBinh'
    if 'Quảng Trị' in x:
        return 'QuangTri'
    if 'Sóc Trăng' in x:
        return 'SocTrang'
    if 'Sơn La' in x:
        return 'SonLa'
    if 'Tây Ninh' in x:
        return 'TayNinh'
    if 'Thái Nguyên' in x:
        return 'ThaiNguyen'
    if 'Trà Vinh' in x:
        return 'TraVinh'
    if 'Tuyên Quang' in x:
        return 'TuyenQuang'
    if 'Vĩnh Long' in x:
        return 'VinhLong'
    if 'Yên Bái' in x:
        return 'YenBai'

    return "not-found"
