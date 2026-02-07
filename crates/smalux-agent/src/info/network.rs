use std::time::Duration;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use futures::FutureExt;
use reqwest::{dns, Client, Error, Response};
use serde::{Deserialize, Serialize};
use sysinfo::Networks;
use smalux_core::model::info::{Ip,NetworkInfo,Network};
use futures::stream::{FuturesOrdered,StreamExt};
use serde_json::Value;
use serde_json_path::JsonPath;

pub(crate) async fn get_network_info(networks: &mut Networks) -> anyhow::Result<NetworkInfo>{
    networks.refresh(true);
    let mut res_network = NetworkInfo::default();

    for (name, network) in networks.list() {
        let mut netword_info = Network::default();
        netword_info.name = name.to_string();
        netword_info.mtu = network.mtu();
        netword_info.received = network.received();
        netword_info.errors_on_received=network.errors_on_received();
        netword_info.errors_on_transmitted = network.errors_on_transmitted();
        netword_info.mac  = network.mac_address().to_string();
        netword_info.packets_received  = network.packets_received();
        netword_info.transmitted = network.transmitted();

        netword_info.total_received = network.total_received();
        netword_info.total_errors_on_received = network.total_errors_on_received();
        netword_info.total_errors_on_transmitted = network.total_errors_on_transmitted();
        netword_info.total_packets_received = network.total_packets_received();
        netword_info.total_transmitted = network.total_transmitted();

        //统计全部
        res_network.received += netword_info.received;
        res_network.errors_on_received+=netword_info.errors_on_received;
        res_network.errors_on_transmitted += netword_info.errors_on_transmitted;
        res_network.packets_received  += netword_info.packets_received;
        res_network.transmitted += netword_info.transmitted;
        
        res_network.total_received += netword_info.total_received;
        res_network.total_errors_on_received += netword_info.total_errors_on_received;
        res_network.total_errors_on_transmitted += netword_info.total_errors_on_transmitted;
        res_network.total_packets_received += netword_info.total_packets_received;
        res_network.total_transmitted += netword_info.total_transmitted;


        for ip in network.ip_networks() {
            let mut ip_info = Ip::default();
            ip_info.mask_len = ip.prefix;
            ip_info.ip = ip.addr;
            netword_info.ip.push(ip_info);
        }
        res_network.networks.push(netword_info);
    }

    Ok(res_network)
}



pub(crate) async  fn  get_public_network() -> anyhow::Result<Vec<IpAddr>> {
    let mut ips = vec![];
    let (v4,v6) = futures::join!(get_public_network_v4(),get_public_network_v6());
    match v4 {
        Ok(ipv4) => {
            ips.push(ipv4);
        }
        Err(_) => {}
    }

    match v6 {
        Ok(ipv6) => {
            ips.push(ipv6);
        }
        Err(_) => {}
    }
    Ok(ips)
}


pub async fn fetch_public_network(client:reqwest::Client,url:&str,json_path: Option<JsonPath>) -> anyhow::Result<IpAddr> {
    let body =  client.get(url)
        .send()
        .await?
        .text()
        .await?;
    match json_path {
        None => {
            Ok(IpAddr::from_str(&body)?)
        }
        Some(ip_json_path) => {
            let ip_json = serde_json::from_str(body.as_str())?;
            let ip_node = ip_json_path.query(&ip_json);
            match ip_node.first() {
                None => {
                    anyhow::bail!("No node found with given IP address");
                }
                Some(ip_node) => {
                    match ip_node.as_str() {
                        None => {
                            anyhow::bail!("{} conversion to ip error", ip_node);
                        }
                        Some(ip_node_str) => {
                            Ok(IpAddr::from_str(ip_node_str)?)
                        }
                    }

                }
            }
        }
    }
}

pub async fn fetch_public_networks(verify_url:Vec<(&str,Option<JsonPath>)>) -> anyhow::Result<IpAddr> {

    let client = reqwest::Client::builder()
        .build()?;

    let mut in_flight = futures::stream::FuturesOrdered::new();

    let mut verify_url_iter = verify_url.iter();
    for _ in 0..2 {
        if let Some((url,json_path)) = verify_url_iter.next() {
            in_flight.push_back(fetch_public_network(client.clone(), url,json_path.clone()).boxed());
        }
    }


    // 轮询当前并发，成功则返回，否则补充新的请求
    while let Some(res) = in_flight.next().await {
        match res {
            Ok(ip) => {
                return Ok(ip); // 一旦成功直接返回，并 cancel 掉剩余 futures
            }
            Err(err) => {
                eprintln!("失败: {:?}", err);
            }
        }

        // 补充一个新的任务到并发中
        if let Some((url,json_path)) = verify_url_iter.next() {
            let c = client.clone();
            in_flight.push_back(fetch_public_network(c, url,json_path.clone()).boxed());
        }
    }
    anyhow::bail!("Failed to fetch public networks");
}
pub(crate) async fn get_public_network_v4() ->anyhow::Result<IpAddr>{
    let v4_verify_url = vec![
        ("https://api.ipify.org",None),
        ("https://4.ipconfig.com",None),
        ("https://ifconfig.me/ip",None),
        ("https://4.ident.me",None),
        ("https://api.myip.la",None),
        ("https://api64.ipify.org",None),
        ("https://ipv4.ip.sb",None)
    ];
    fetch_public_networks(v4_verify_url).await
}

pub(crate) async fn get_public_network_v6() -> anyhow::Result<IpAddr> {
    let v6_verify_url = vec![
        ("https://api6.ipify.org",None),
        ("https://6.ipconfig.com",None),
        ("https://6.ident.me",None),
        ("https://ipv6.ip.sb",None)
    ];
    fetch_public_networks(v6_verify_url).await

}