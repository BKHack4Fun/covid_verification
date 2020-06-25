from nlp.src.predict import ner_sent, load_model, ner_sent2
from nlp.src.preprocess_raw import preprocess_raw
from nlp.src.extract import extract_info
from config.config import NLPConfig

my_config = NLPConfig()
model_dir = my_config.model_path
data_dir = my_config.data_path

p3 = """THÔNG BÁO VỀ 2 BN336 - BN342 : 
BN336: bệnh nhân nam, 29 tuổi, có địa chỉ tại Nghi Lộc, Nghệ An; 
BN337: bệnh nhân nam, 25 tuổi, có địa chỉ tại Hoằng Hoá, Thanh Hoá; 
BN338: bệnh nhân nam, 38 tuổi, có địa chỉ tại Lạng Giang, Bắc Giang; 
BN339: bệnh nhân nam, 37 tuổi, có địa chỉ tại Diễn Châu, Nghệ An; 
BN340: bệnh nhân nam, 33 tuổi, có địa chỉ tại Yên Thành, Nghệ An; 
BN341: bệnh nhân nam, 35 tuổi, có địa chỉ tại Tứ Kỳ, Hải Dương; 
BN342: bệnh nhân nam, 41 tuổi, có địa chỉ tại Hương Khê, Hà Tĩnh. 
Ngày 16/6 các bệnh nhân này từ Kuwait (quá cảnh Quatar) về sân bay Tân 
Sơn Nhất, TP. Hồ Chí Minh trên chuyến bay QH9092 và được cách ly ngay tại 
khu cách ly thị xã Phú Mỹ, tỉnh Bà Rịa-Vũng Tàu. Ngày 17/6, các bệnh nhân 
được lấy mẫu xét nghiệm. Kết quả xét nghiệm ngày 17/6 là dương tính với SARS-CoV-2. 
Hiện 7 bệnh nhân được cách ly, điều trị tại Bệnh viện Bà Rịa."""

p7 = """THÔNG BÁO VỀ CA BỆNH 223-227: BN223: nữ, 29 tuổi, địa chỉ: Hải Hậu, Nam Định, 
chăm sóc người thân tại Khoa Phục hồi chức năng, Bệnh viện Bạch Mai từ 11/3. 
Từ 11/3-24/3 bệnh nhân thường xuyên đi ăn uống và mua đồ tạp hoá ở căng tin, 
có tiếp xúc với đội cung cấp nước sôi của Công ty Trường Sinh; BN224: nam, 39 tuổi, 
quốc tịch Brazil, có địa chỉ tại phường Thảo Điền, Q.2, TP Hồ Chí Minh. Bệnh nhân có thời 
gian sống cùng phòng với BN158 tại chung cư Masteri, không có triệu chứng lâm sàng; BN225: 
nam, 35 tuổi, quê An Đông, An Dương, Hải Phòng, làm việc ở Matxcova Nga 10 năm nay, về nước trên 
chuyến bay SU290 ghế 50D ngày 25/3/2020 và được cách ly tại Đại học FPT- Hòa Lạc, Thạch Thất; 
BN226: nam, 22 tuổi, về nước cùng chuyến bay với BN 212 ngày 27/3 và được cách ly tại Trường Văn 
hóa Nghệ thuật Vĩnh Phúc; BN227: nam, 31 tuổi, là con của BN209, có tiếp xúc gần tại gia đình trong 
khoảng thời gian từ 16-25/3."""

p9 = """THÔNG BÁO VỀ 4 CA BỆNH 246 - 249: BN246 - nam, 33 tuổi, ở huyện Yên Thành, Nghệ An, 
làm đầu bếp tại Moscow (Nga), ngày 24/3 từ Nga trở về Việt Nam trên chuyến bay SU290 (ghế 49F), 
nhập cảnh Nội Bài ngày 25/3; BN247 - nam, 28 tuổi, ở phường 1, quận Bình Thạnh, TP.HCM, 
là đồng nghiệp, có tiếp xúc gần với BN124 và BN151; BN248 - nam, 20 tuổi, quốc tịch Việt Nam, 
từ Mỹ quá cảnh Nhật Bản về Việt Nam trên chuyến bay JL079 ngày 23/3 nhập cảnh tại Tân Sơn Nhất; 
BN249 - nam, 55 tuổi, quốc tịch Việt Nam, từ Mỹ quá cảnh tại Hồng Kông, nhập cảnh ngày 22/3, 
khởi phát bệnh tại Mỹ."""

model = load_model(model_dir + 'covid_ner.job')
BN_list, triplets = extract_info(p9, model)
print('patient_list')
for bn in BN_list:
    print(bn)
print('\ntriplets + time')
for triplet in triplets:
    print(triplet)