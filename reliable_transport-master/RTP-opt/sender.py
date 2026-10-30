import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

LOAI_BAT_DAU = 0
LOAI_KET_THUC = 1
LOAI_DU_LIEU = 2
LOAI_XAC_NHAN = 3

KICH_THUOC_TOI_DA = 1456
THOI_GIAN_CHO = 0.5


def tao_goi_tin_rtp(loai_goi, so_thu_tu, du_lieu_kem=b""):
    do_dai = len(du_lieu_kem)
    phan_dau = PacketHeader(type=loai_goi, seq_num=so_thu_tu, length=do_dai, checksum=0)
    phan_dau.checksum = compute_checksum(phan_dau / du_lieu_kem)
    return bytes(phan_dau / du_lieu_kem)


def giai_ma_goi_phan_hoi(du_lieu_tho):
    if len(du_lieu_tho) < 16:
        return None
    phan_dau = PacketHeader(du_lieu_tho[:16])
    if phan_dau.length < 0 or 16 + phan_dau.length > len(du_lieu_tho):
        return None
    phan_than = du_lieu_tho[16 : 16 + phan_dau.length]

    ma_kiem_tra_goc = phan_dau.checksum
    phan_dau.checksum = 0
    if compute_checksum(phan_dau / phan_than) != ma_kiem_tra_goc:
        return None
    return phan_dau


def cat_du_lieu_dau_vao():
    luong_nhap = sys.stdin.buffer.read()
    tong_so_byte = len(luong_nhap)
    cac_manh_du_lieu = []
    vi_tri = 0
    while vi_tri < tong_so_byte:
        cac_manh_du_lieu.append(luong_nhap[vi_tri : vi_tri + KICH_THUOC_TOI_DA])
        vi_tri += KICH_THUOC_TOI_DA
    return cac_manh_du_lieu


def sender(receiver_ip, receiver_port, window_size):
    """Mo socket UDP va truyen du lieu voi co che ACK rieng le (RTP-opt)."""
    cong_ket_noi = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dia_chi_dich = (receiver_ip, receiver_port)

    danh_sach_manh = cat_du_lieu_dau_vao()
    tong_so_goi = len(danh_sach_manh)

    kho_goi_tin = {}
    for chi_so in range(tong_so_goi):
        stt_goi = chi_so + 1
        kho_goi_tin[stt_goi] = tao_goi_tin_rtp(LOAI_DU_LIEU, stt_goi, danh_sach_manh[chi_so])

    goi_khoi_tao = tao_goi_tin_rtp(LOAI_BAT_DAU, 0)
    cong_ket_noi.settimeout(0.1)
    da_ket_noi = False
    while not da_ket_noi:
        cong_ket_noi.sendto(goi_khoi_tao, dia_chi_dich)
        try:
            phan_hoi_tho, _ = cong_ket_noi.recvfrom(2048)
            dau_goi_ack = giai_ma_goi_phan_hoi(phan_hoi_tho)
            if dau_goi_ack is not None and dau_goi_ack.type == LOAI_XAC_NHAN:
                if dau_goi_ack.seq_num == 1:
                    da_ket_noi = True
        except socket.timeout:
            pass

    dau_cua_so = 1
    goi_tiep_theo = 1
    tap_da_xac_nhan = set()
    moc_thoi_gian = time.time()

    while dau_cua_so <= tong_so_goi:
        gioi_han_cua_so = min(dau_cua_so + window_size, tong_so_goi + 1)
        while goi_tiep_theo < gioi_han_cua_so:
            cong_ket_noi.sendto(kho_goi_tin[goi_tiep_theo], dia_chi_dich)
            goi_tiep_theo += 1

        thoi_gian_con_lai = THOI_GIAN_CHO - (time.time() - moc_thoi_gian)
        if thoi_gian_con_lai <= 0:
            for stt in range(dau_cua_so, gioi_han_cua_so):
                if stt not in tap_da_xac_nhan:
                    cong_ket_noi.sendto(kho_goi_tin[stt], dia_chi_dich)
            moc_thoi_gian = time.time()
            continue

        cong_ket_noi.settimeout(thoi_gian_con_lai)
        try:
            phan_hoi_tho, _ = cong_ket_noi.recvfrom(2048)
            dau_goi_ack = giai_ma_goi_phan_hoi(phan_hoi_tho)
            if dau_goi_ack is not None and dau_goi_ack.type == LOAI_XAC_NHAN:
                stt_xac_nhan = dau_goi_ack.seq_num
                if stt_xac_nhan >= dau_cua_so:
                    tap_da_xac_nhan.add(stt_xac_nhan)
                    if dau_cua_so in tap_da_xac_nhan:
                        while dau_cua_so in tap_da_xac_nhan:
                            dau_cua_so += 1
                        moc_thoi_gian = time.time()
        except socket.timeout:
            for stt in range(dau_cua_so, gioi_han_cua_so):
                if stt not in tap_da_xac_nhan:
                    cong_ket_noi.sendto(kho_goi_tin[stt], dia_chi_dich)
            moc_thoi_gian = time.time()

    stt_ket_thuc = tong_so_goi + 1
    goi_ket_thuc = tao_goi_tin_rtp(LOAI_KET_THUC, stt_ket_thuc)
    moc_ket_thuc = time.time()

    while True:
        con_lai = THOI_GIAN_CHO - (time.time() - moc_ket_thuc)
        if con_lai <= 0:
            break
        cong_ket_noi.sendto(goi_ket_thuc, dia_chi_dich)
        cong_ket_noi.settimeout(min(0.1, con_lai))
        try:
            phan_hoi_tho, _ = cong_ket_noi.recvfrom(2048)
            dau_goi_ack = giai_ma_goi_phan_hoi(phan_hoi_tho)
            if dau_goi_ack is not None and dau_goi_ack.type == LOAI_XAC_NHAN:
                if dau_goi_ack.seq_num >= stt_ket_thuc:
                    break
        except socket.timeout:
            pass

    cong_ket_noi.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
